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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MainDummyActionTest extends MainTest{

    private String input;
    private String action;
    private String nowInstant;

    @Override
    protected String getOperationCMD() {
        input = "http://www.jewishfederations.org/local_includes/downloads/39497.pdf";
        action = "FORTESTING";
        nowInstant = Instant.now().toString();
        return String.join(" ",
                "-a", action,
                "-i", input,
                "-b", outputS3Bucket,
                "-k", getKey(),
                "-t", nowInstant
        );
    }

    @Override
    protected String[] getExpectedOutputMsg() {
        return new String[]{"-a",action,"-s","SUCCESS","-t", nowInstant,"-i",input,"-u",String.format("https://%s.s3.amazonaws.com/%s", outputS3Bucket,getKey()),"-d","NOT_TESTING"};
    }

    @Override
    protected String getKey() {
        return "someKey";
    }
}