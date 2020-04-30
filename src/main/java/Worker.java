import logging.InfoLogger;
import logging.SeverLogger;
import org.apache.commons.cli.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.nio.file.Paths;

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

    public OperationResult accept(OperationMessage.ToImage toImage) throws NotImplementedException {
        infoLogger.log("Handling ToImage");
        throw new NotImplementedException();
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
        return new OperationResult();
    }

    private OperationResult process(OperationMessage operationMessage) throws OperationMessage.UnfamiliarActionException, NotImplementedException {
        Thread visibilityMenager = new Thread(new VisibilityManagement(initialVisibilityTimeout, operationMessage.getMessage(), sqsClient, operationsSqsName, infoLogger, severLogger));
        visibilityMenager.start();
        OperationResult res = operationMessage.getAction().visit(this);
        visibilityMenager.interrupt();
        return res;
    }

    private OperationMessage receive() throws NoMoreOperationsException, ParseException {
        infoLogger.log("receiving");
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(this.operationsSqsName)
                .build();
        String queueUrl = sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();
        ReceiveMessageResponse receiveMessageResponse = null;
        infoLogger.log("busy waiting for message");
        while (true){
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
        infoLogger.log("posting");
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(message.getBucket())
                .key(message.getKey())
                .acl("public-read")
                .build();
        PutObjectResponse putObjectResponse = s3Client.putObject(
                putObjectRequest,
                Paths.get(System.getProperty("user.dir"), "test_files", "input", "key.jpg")
        );
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
        if (operationResult.isSuccess()) {
            sendMessageRequest = builder
                    .messageBody(String.join(" ",
                            "-a", message.getActionString(),
                            "-i", message.getInput(),
                            "-s", "SUCCESS",
                            "-u", String.format(String.format("https://%s.s3.amazonaws.com/%s",message.getBucket(), message.getKey()))
                            )
                    ).build();
        }else{
            sendMessageRequest = builder
                    .messageBody(String.join(" ",
                            "-a", message.getActionString(),
                            "-i", message.getInput(),
                            "-s", "FAIL",
                            "-u", String.format(String.format("https://%s.s3.amazonaws.com/%s",message.getBucket(), message.getKey()))
                            )
                    ).build();
        }
        sqsClient.sendMessage(sendMessageRequest);
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
                notifai(result,currentOperationMessage);
                infoLogger.log("Done handling a message");
            } catch (NotImplementedException e) {
                severLogger.log("Not implemented yet", e);
            } catch (OperationMessage.UnfamiliarActionException e) {
                severLogger.log("Unfamiliar action received", e);
            } catch (ParseException e) {
                severLogger.log("Failed parsing the operation", e);
            } catch (NoMoreOperationsException e) {
                severLogger.log("No more operations. The worker is idle", e);
            }catch (Exception e) {
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
