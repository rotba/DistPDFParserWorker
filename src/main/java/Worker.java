import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Worker {
    private String action;
    private String input;
    private String pushNotificationsSqsName;
    private String s3OutputBucketAddress;
    private String s3OKey;
    private InfoLogger infoLogger;
    private SeverLogger severLoggerLogger;
    private SqsClient pushNotificationsSqs;
    private S3Client s3OutputBucket;

    public Worker(String action, String input, String pushNotificationsSqsName, String s3OutputBucketAddress, String s3OKey, Region region, InfoLogger infoLogger, SeverLogger severLoggerLogger) {
        this.action = action;
        this.input = input;
        this.pushNotificationsSqsName = pushNotificationsSqsName;
        this.s3OutputBucketAddress = s3OutputBucketAddress;
        this.s3OKey = s3OKey;
        this.infoLogger = infoLogger;
        this.severLoggerLogger = severLoggerLogger;
        this.pushNotificationsSqs = SqsClient.builder().region(region).build();
        this.s3OutputBucket = S3Client.builder().region(region).build();
    }

    public void work() {
        infoLogger.log("Started working");
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3OutputBucketAddress)
                .key(s3OKey)
                .acl("public-read")
                .build();
        PutObjectResponse putObjectResponse = s3OutputBucket.putObject(
                putObjectRequest,
                Paths.get(System.getProperty("user.dir"), "test_files","input","key")
        );

        infoLogger.log("Worker put the result in the s3");
        GetQueueUrlRequest gquRequest = GetQueueUrlRequest.builder()
                .queueName(pushNotificationsSqsName)
                .build();
        String qUrl = pushNotificationsSqs.getQueueUrl(gquRequest).queueUrl();
        SendMessageRequest smRequest = SendMessageRequest.builder()
                .messageBody(String.format("%s https://%s.s3.amazonaws.com/%s",action,s3OutputBucketAddress,s3OKey))
                .queueUrl(qUrl)
                .build();
        pushNotificationsSqs.sendMessage(smRequest);
        infoLogger.log("Sent finish msg to the notifications q");
        infoLogger.log("Done");
    }
}
