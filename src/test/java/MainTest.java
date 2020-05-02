import org.apache.commons.cli.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.*;

public abstract class MainTest {

    private Thread theMainThread;
    private String pushNotoficationsSqs;
    protected String outputS3Bucket;
//    protected String outputS3Key;
    private SqsClient sqs;
    private S3Client s3;
    private String operationsSqs;


    @Before
    public void setUp() throws Exception {
        pushNotoficationsSqs = "rotemb271TestPushNotificationsSqs"+new Date().getTime();
        operationsSqs = "rotemb271TestPushOperationsSqs"+ new Date().getTime();
        outputS3Bucket = "rotemb271-test-output-bucket2";
//        outputS3Key = "rotemb271TestOutputKey";
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        sqs.createQueue(CreateQueueRequest.builder()
                .queueName(pushNotoficationsSqs)
                .build());
        sqs.createQueue(CreateQueueRequest.builder()
                .queueName(operationsSqs)
                .build());
        s3 = S3Client.builder().build();
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(outputS3Bucket)
                .build());
    }

    private void emptySQS(String sqsName) {
        String queueUrl = sqs.getQueueUrl(
                GetQueueUrlRequest.builder()
                        .queueName(sqsName)
                        .build()
        ).queueUrl();

        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();
        sqs.deleteQueue(deleteQueueRequest);
    }

    @After
    public void tearDown() throws Exception {
        theMainThread.interrupt();
        int i = 5;
        emptySQS(pushNotoficationsSqs);
        emptySQS(operationsSqs);
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        Ec2Client ec2Client = Ec2Client.builder().build();
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        for (Reservation r: response.reservations()) {
            for(Instance instance: r.instances()){
                if(isWorker(instance)){
                    TerminateInstancesRequest terminateInstancesRequest = TerminateInstancesRequest.builder()
                            .instanceIds(instance.instanceId())
                            .build();
                }
            }
        }
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(outputS3Bucket)
                .key(getKey())
                .build();
        s3.deleteObject(deleteObjectRequest);
        Runtime rt = Runtime.getRuntime();
        try {
            Process pr = rt.exec(String.format("rm %s" ,Paths.get("test_files","output", getKey())));
            pr.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean isWorker(Instance instance) {
        for (Tag t:instance.tags()) {
            if (t.key().equals("Name") && t.value().equals("Worker"))
                return true;
        }
        return false;
    }

    private boolean equivalentCommands(String body, String[] expected) {
        Options operationParsingOptions = new Options();
        Option action = new Option("a", "action", true, "action");
        action.setRequired(true);
        operationParsingOptions.addOption(action);
        Option input = new Option("i", "input", true, "input file");
        input.setRequired(true);
        operationParsingOptions.addOption(input);
        Option status = new Option("s", "status", true, "status");
        status.setRequired(true);
        operationParsingOptions.addOption(status);
        Option url = new Option("u", "url", true, "result url");
        url.setRequired(true);
        operationParsingOptions.addOption(url);
        Option timestamp = new Option("t", "timestamp", true, "timestamp");
        timestamp.setRequired(true);
        operationParsingOptions.addOption(timestamp);
        Option description = new Option("d", "description", true, "description");
        description.setRequired(true);
        operationParsingOptions.addOption(description);
        CommandLineParser operationParser = new DefaultParser();
        try {
            CommandLine expectedCmd = operationParser.parse(operationParsingOptions, expected);
            CommandLine operationResultCmd = operationParser.parse(operationParsingOptions, body.split("\\s+"));
            return expectedCmd.getOptionValue("a").equals(operationResultCmd.getOptionValue("a")) &&
                    expectedCmd.getOptionValue("i").equals(operationResultCmd.getOptionValue("i")) &&
                    expectedCmd.getOptionValue("s").equals(operationResultCmd.getOptionValue("s")) &&
                    expectedCmd.getOptionValue("u").equals(operationResultCmd.getOptionValue("u")) &&
                    expectedCmd.getOptionValue("t").equals(operationResultCmd.getOptionValue("t"));
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean sqsContainsCommand(String pushNotoficationsSqs, String[] command) {
//        SqsClient sqs = SqsClient.builder().region(Region.of(Main.region)).build();
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(pushNotoficationsSqs)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        for (Message m:sqs.receiveMessage(receiveRequest).messages()) {
            if(equivalentCommands(m.body(), command))
                return true;
        }
        return false;
    }

    private File download(String address, String key) {
        Runtime rt = Runtime.getRuntime();
        try {
            Process pr = rt.exec(String.format("wget %s -P %s" ,address, Paths.get("test_files","output")));
            pr.waitFor();
            return new File(Paths.get(System.getProperty("user.dir"),key).toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        fail();
        return null;
    }

    private boolean isImage(File download) {
        return download!=null;
    }

    @Test
    public void main() {
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(operationsSqs).build()).queueUrl())
                        .messageBody(getOperationCMD())
                        .build()
        );
        theMainThread = new Thread(() -> {
            try {
                Main.main(
                        new String[]{
                                "-n",pushNotoficationsSqs,
                                "-o",operationsSqs,
                        });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        theMainThread.start();
        Utils.waitParsingTime();
        theMainThread.interrupt();
        assertTrue(
                sqsContainsCommand(
                        pushNotoficationsSqs,
                        getExpectedOutputMsg()
                )
        );
        assertTrue(isImage(download(String.format("https://%s.s3.amazonaws.com/%s", outputS3Bucket, getKey()),getKey())));
    }

    protected abstract String getOperationCMD();
    protected abstract String getKey();
    protected abstract String[] getExpectedOutputMsg();
}