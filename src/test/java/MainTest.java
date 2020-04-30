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

import static org.junit.Assert.*;

public class MainTest {

    private Thread theMainThread;
    private String pushNotoficationsSqs;
    private String outputS3Bucket;
    private String outputS3Key;
    private String region;
    private SqsClient sqs;
    private S3Client s3;
    private String operationsSqs;
    private String operationMsgId;


    @Before
    public void setUp() throws Exception {
        pushNotoficationsSqs = "rotemb271TestPushNotificationsSqs";
        operationsSqs = "rotemb271TestPushOperationsSqs";
        outputS3Bucket = "rotemb271-test-output-bucket2";
        outputS3Key = "rotemb271TestOutputKey";
        region = String.format("s3-%s",Main.region);
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
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .build();
        for (Message m:sqs.receiveMessage(receiveRequest).messages()) {
            DeleteMessageRequest delete = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(delete);
        }
    }

    @After
    public void tearDown() throws Exception {
        theMainThread.interrupt();
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
                .key(outputS3Key)
                .build();
        s3.deleteObject(deleteObjectRequest);
        Runtime rt = Runtime.getRuntime();
        try {
            Process pr = rt.exec(String.format("rm %s" ,Paths.get("test_files","output", outputS3Key)));
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
        CommandLineParser operationParser = new DefaultParser();
        try {
            CommandLine expectedCmd = operationParser.parse(operationParsingOptions, body.split("\\s+"));
            CommandLine operationResultCmd = operationParser.parse(operationParsingOptions, body.split("\\s+"));
            return expectedCmd.getOptionValue("a").equals(operationResultCmd.getOptionValue("a")) &&
                    expectedCmd.getOptionValue("i").equals(operationResultCmd.getOptionValue("i")) &&
                    expectedCmd.getOptionValue("s").equals(operationResultCmd.getOptionValue("s")) &&
                    expectedCmd.getOptionValue("u").equals(operationResultCmd.getOptionValue("u"));
        } catch (ParseException e) {
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

    private File download(String address, String outputS3Key) {
        Runtime rt = Runtime.getRuntime();
        try {
            Process pr = rt.exec(String.format("wget %s -P %s" ,address, Paths.get("test_files","output")));
            pr.waitFor();
            return new File(Paths.get(System.getProperty("user.dir"),outputS3Key).toString());
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
        String input = "http://www.jewishfederations.org/local_includes/downloads/39497.pdf";
        String action = "FORTESTING";
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(operationsSqs).build()).queueUrl())
                        .messageBody(String.join(" ",
                                "-a", action,
                                "-i", input,
                                "-b", outputS3Bucket,
                                "-k", outputS3Key
                        ))
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
                        new String[]{"-a",action,"-s","SUCCESS","-i",input,"-u",String.format("https://%s.s3.amazonaws.com/%s", outputS3Bucket, outputS3Key)}
                )
        );
        assertTrue(isImage(download(String.format("https://%s.s3.amazonaws.com/%s", outputS3Bucket, outputS3Key),outputS3Key)));
    }
}