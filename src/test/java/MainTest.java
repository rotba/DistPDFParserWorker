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

import javax.imageio.ImageIO;
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

    @Before
    public void setUp() throws Exception {
        pushNotoficationsSqs = "rotemb271TestPushNotificationsSqs";
        outputS3Bucket = "rotemb271-test-output-bucket2";
        outputS3Key = "rotemb271TestOutputKey";
        region = String.format("s3-%s",Main.region);
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        CreateQueueRequest createQueueRequest  = CreateQueueRequest.builder()
                .queueName(pushNotoficationsSqs)
                .build();
        sqs.createQueue(createQueueRequest);
        s3 = S3Client.builder().build();
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(outputS3Bucket)
                .build());
    }

    @After
    public void tearDown() throws Exception {
        theMainThread.interrupt();
        SqsClient sqs = SqsClient.builder().region(Main.region).build();
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(pushNotoficationsSqs)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        for (Message m:sqs.receiveMessage(receiveRequest).messages()) {
            DeleteMessageRequest delete = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(delete);
        }

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

    private boolean sqsContainsMsg(String pushNotoficationsSqs, String msg) {
//        SqsClient sqs = SqsClient.builder().region(Region.of(Main.region)).build();
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(pushNotoficationsSqs)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        for (Message m:sqs.receiveMessage(receiveRequest).messages()) {
            if(m.body().equals(msg))
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
        theMainThread = new Thread(() -> {
            try {
                Main.main(
                        new String[]{
                                "-a","ToImage",
                                "-i","http://www.jewishfederations.org/local_includes/downloads/39497.pdf",
                                "-o",pushNotoficationsSqs,
                                "-b", outputS3Bucket,
                                "-k", outputS3Key
                        });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        theMainThread.start();
        Utils.waitParsingTime();
        assertTrue(sqsContainsMsg(pushNotoficationsSqs, String.format("ToImage https://%s.s3.amazonaws.com/%s", outputS3Bucket, outputS3Key)));
        assertTrue(isImage(download(String.format("https://%s.s3.amazonaws.com/%s", outputS3Bucket, outputS3Key),outputS3Key)));
    }
}