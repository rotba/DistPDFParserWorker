import logging.InfoLogger;
import logging.SeverLogger;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Worker {
    private static final int REQUIRED_PAGE_INDEX = 0;
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

    private OperationMessage receive() throws NoMoreOperationsException, ParseException, OperationMessage.UnfamiliarActionException {
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
                    Paths.get(result.getOutputPath())
            );
        } else {
            infoLogger.log("Not posting anything");
        }
    }

    private void clean(OperationResult result, OperationMessage currentOperationMessage) {
        infoLogger.log("cleaning");
        if(currentOperationMessage.getAction() instanceof OperationMessage.FORTESTING) return;
        try {
            Files.deleteIfExists(new File(result.getInputPath()).toPath()); //
            Files.deleteIfExists(new File(result.getOutputPath()).toPath()); //
        } catch (IOException e) {
            severLogger.log("Troubles cleaning. NOT stopping worker", e);
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


    /**
     * @preconditions: the output file path encapsulated in  toImage.Key() ends with .png
     */
    public OperationResult accept(OperationMessage.ToImage toImage) throws NotImplementedException {
        infoLogger.log("Handling ToImage");
        Runtime rt = Runtime.getRuntime();
        String fileInputPath = Paths.get(System.getProperty("user.dir"), "input_files", FilenameUtils.getName(toImage.getInput())).toString();
        String outputPath = Paths.get(System.getProperty("user.dir"), "output_files", toImage.getKey()).toString();
        try {
            Process pr = rt.exec(String.format("wget %s -P %s", toImage.getInput(), Paths.get("input_files")));
            pr.waitFor();
//            PDDocument document = PDDocument.load(new File(fileInputPath));
            PDDocument wholeDocument = PDDocument.load(new File(fileInputPath));
            PDDocument theFirstPageDocument = new PDDocument();
            theFirstPageDocument.addPage(wholeDocument.getPage(0));
            PDFRenderer pdfRenderer = new PDFRenderer(theFirstPageDocument);
            BufferedImage bim = pdfRenderer.renderImageWithDPI(REQUIRED_PAGE_INDEX, 300, ImageType.RGB);
            // suffix in filename will be used as the file format
            ImageIOUtil.writeImage(bim, outputPath, 300);
            wholeDocument.close();
            theFirstPageDocument.close();
            return new OperationResult(outputPath,fileInputPath);
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

    public OperationResult accept(OperationMessage.ToText toText)  {
        infoLogger.log("Handling ToText");
        Runtime rt = Runtime.getRuntime();
        String fileInputPath = Paths.get(System.getProperty("user.dir"), "input_files", FilenameUtils.getName(toText.getInput())).toString();
        String outputPath = Paths.get(System.getProperty("user.dir"), "output_files", toText.getKey()).toString();
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            Process pr = rt.exec(String.format("wget %s -P %s", toText.getInput(), Paths.get("input_files")));
            pr.waitFor();
            PDDocument wholeDocument = PDDocument.load(new File(fileInputPath));
            PDDocument theFirstPageDocument = new PDDocument();
            theFirstPageDocument.addPage(wholeDocument.getPage(0));
            writer.write(new PDFTextStripper().getText(theFirstPageDocument));
            wholeDocument.close();
            theFirstPageDocument.close();
            return new OperationResult(outputPath,fileInputPath);
        } catch (IOException e) {
            severLogger.log("Fail handling ToImage", e);
            return new FailedOperationResult("Fail handling ToImage", e);
        } catch (InterruptedException e) {
            severLogger.log("Fail handling ToImage", e);
            return new FailedOperationResult("Fail handling ToImage", e);
        }
    }

    public OperationResult accept(OperationMessage.FORTESTING fortesting) throws NotImplementedException {
        infoLogger.log("Handling FORTESTING");
        return new OperationResult(Paths.get(System.getProperty("user.dir"), "test_files", "input", "key.jpg").toString(),null);
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
                clean(result, currentOperationMessage);
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
