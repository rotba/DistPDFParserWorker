import logging.InfoLogger;
import logging.SeverLogger;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

public class Worker {
    private String pushNotificationsSqsName;
    private final String operationsSqsName;
    private final int initialVisibilityTimeout;
    private InfoLogger infoLogger;
    private SeverLogger severLogger;
    private SqsClient sqsClient;
    private S3Client s3Client;

    public Worker(String pushNotificationsSqsName, String operationsSqsName, Region region, int initialVisibilityTimeout, InfoLogger infoLogger, SeverLogger severLogger) {
        this.pushNotificationsSqsName = pushNotificationsSqsName;
        this.operationsSqsName = operationsSqsName;
        this.initialVisibilityTimeout = initialVisibilityTimeout;
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
        this.sqsClient = SqsClient.builder().region(region).build();
        this.s3Client = S3Client.builder().region(region).build();

    }

    private OperationMessage receive() throws NoMoreOperationsException, ParseException {
        infoLogger.log("receiving");
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(this.operationsSqsName)
                .build();
        String queueUrl = sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();
        ReceiveMessageResponse receiveMessageResponse = null;
        infoLogger.log("busy waiting for message");
        while (true) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .visibilityTimeout(initialVisibilityTimeout)
                    .maxNumberOfMessages(1)
                    .queueUrl(queueUrl)
                    .build();
            receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);
            if (receiveMessageResponse.messages().size() > 0)
                break;

        }
        infoLogger.log(String.format("Got message %s", receiveMessageResponse.messages().get(0).body()));
        return new OperationMessage(receiveMessageResponse.messages().get(0));
    }

    private void post(OperationResult result, OperationMessage message) {
        if (result.isSuccess()) {
            infoLogger.log("posting");
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(message.getBucket())
                    .key(message.getKey())
                    .acl("public-read")
                    .build();
            infoLogger.log("posting");
            PutObjectResponse putObjectResponse = s3Client.putObject(
                    putObjectRequest,
                    Paths.get(result.filePath())
            );
        } else {
            infoLogger.log("Not posting anything");
        }
    }

    private void delete(OperationMessage currentOperationMessage) {
        infoLogger.log("deleting");
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(operationsSqsName).build()).queueUrl())
                .receiptHandle(currentOperationMessage.getMessage().receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);

    }

    private void notifai(OperationResult operationResult, OperationMessage message) {
        infoLogger.log("notifing");
        SendMessageRequest.Builder builder = SendMessageRequest.builder()
                .queueUrl(sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(pushNotificationsSqsName).build()).queueUrl());
        SendMessageRequest sendMessageRequest = null;
        String body = null;
        if (operationResult.isSuccess()) {
            body = String.join(" ",
                    "-a", message.getActionString(),
                    "-i", message.getInput(),
                    "-s", "SUCCESS",
                    "-t", message.getTimeStamp(),
                    "-d", operationResult.getDesc(),
                    "-u", String.format(String.format("https://%s.s3.amazonaws.com/%s", message.getBucket(), message.getKey()))
            );
            sendMessageRequest = builder
                    .messageBody(body
                    ).build();
        } else {
            body = String.join(" ",
                    "-a", message.getActionString(),
                    "-i", message.getInput(),
                    "-t", message.getTimeStamp(),
                    "-s", "FAIL",
                    "-d", operationResult.getDesc(),
                    "-u", String.format(String.format("https://%s.s3.amazonaws.com/%s", message.getBucket(), message.getKey()))
            );
            sendMessageRequest = builder
                    .messageBody(body
                    ).build();
        }
        infoLogger.log(String.format("notifiyng: %s", body));
        sqsClient.sendMessage(sendMessageRequest);
    }

    public OperationResult accept(OperationMessage.ToImage toImage) throws NotImplementedException {
        infoLogger.log("Handling ToImage");
        Runtime rt = Runtime.getRuntime();
        try {
            Process pr = rt.exec(String.format("wget %s -P %s", toImage.getInput(), Paths.get("input_files")));
            pr.waitFor();
            String fileInputPath = Paths.get(System.getProperty("user.dir"), "input_files", FilenameUtils.getName(toImage.getInput())).toString();
            String outputPathNoSuffix = Paths.get(System.getProperty("user.dir"), "output_files", toImage.getKey()).toString();
            PDDocument document = PDDocument.load(new File(fileInputPath));
            PDFRenderer pdfRenderer = new PDFRenderer(document);
            for (int page = 0; page < document.getNumberOfPages(); ++page)
            {
                BufferedImage bim = pdfRenderer.renderImageWithDPI(page, 300, ImageType.RGB);

                // suffix in filename will be used as the file format
                ImageIOUtil.writeImage(bim, outputPathNoSuffix + "-" + (page+1) + ".png", 300);
            }
            document.close();
            return new OperationResult(outputPathNoSuffix);
        } catch (IOException e) {
            severLogger.log("Fail handling ToImage", e);
            return new FailedOperationResult("Fail handling ToImage", e);
        } catch (InterruptedException e) {
            severLogger.log("Fail handling ToImage", e);
            return new FailedOperationResult("Fail handling ToImage", e);
        }
    }

    public OperationResult accept(OperationMessage.ToHTML toHTML) throws NotImplementedException {
        infoLogger.log("Handling ToHTML");
        throw new NotImplementedException();
    }

    public OperationResult accept(OperationMessage.ToText toText) throws NotImplementedException {
        infoLogger.log("Handling ToText");
        throw new NotImplementedException();
    }

    public OperationResult accept(OperationMessage.FORTESTING fortesting) throws NotImplementedException {
        infoLogger.log("Handling FORTESTING");
        return new OperationResult(Paths.get(System.getProperty("user.dir"), "test_files", "input", "key.jpg").toString());
    }

    private OperationResult process(OperationMessage operationMessage) throws OperationMessage.UnfamiliarActionException {
        Thread visibilityMenager = new Thread(new VisibilityManagement(initialVisibilityTimeout, operationMessage.getMessage(), sqsClient, operationsSqsName, infoLogger, severLogger));
        visibilityMenager.start();
        OperationResult res = null;
        try {
            res = operationMessage.getAction().visit(this);
        } catch (NotImplementedException e) {
            severLogger.log("Not impleneted operation", e);
            res = new FailedOperationResult("Operation not implemented",e);
        }
        visibilityMenager.interrupt();
        return res;
    }

    public void work() {
        infoLogger.log("Started working");
        while (true) {
            try {
                infoLogger.log("Started working on a message");
                OperationMessage currentOperationMessage = receive();
                OperationResult result = process(currentOperationMessage);
                post(result, currentOperationMessage);
                delete(currentOperationMessage);
                notifai(result, currentOperationMessage);
                infoLogger.log("Done handling a message");
            } catch (OperationMessage.UnfamiliarActionException e) {
                severLogger.log("Unfamiliar action received", e);
            } catch (ParseException e) {
                severLogger.log("Failed parsing the operation", e);
            } catch (NoMoreOperationsException e) {
                severLogger.log("No more operations. The worker is idle", e);
            } catch (Exception e) {
                severLogger.log("Unknown exception, stop working", e);
                return;
            }
        }

    }

    private class NoMoreOperationsException extends Exception {
    }

    class NotImplementedException extends Exception {
    }
}
