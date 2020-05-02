import java.time.Instant;

public class MainToHTMLTest extends MainTest{

    private String input;
    private String action;
    private String nowInstant;

    @Override
    protected String getOperationCMD() {
        input = "http://www.jfrankhenderson.com/pdf/jesusandseder.PDF";
        action = "ToHTML";
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
        return new String[]{"-a",action,"-s","SUCCESS","-t", nowInstant,"-i",input,"-u",String.format("https://%s.s3.amazonaws.com/%s", outputS3Bucket, getKey()),"-d","NOT_TESTING"};
    }

    @Override
    protected String getKey() {
        return "jesusandseder.html";
    }
}