import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class MainTest {
    private Thread theMainThread;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void main() {
        final String resultsSqs = "rotemb271TestResultsSqs";

        theMainThread = new Thread(()-> {
            try {
                Main.main(new String[]{"ToImage\thttp://www.jewishfederations.org/local_includes/downloads/39497.pdf", resultsSqs});
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        theMainThread.start();
        Utils.waitParsingTime();
        assertTrue(sqsContainsMsg(resultsSqs, "ToImage http://www.jewishfederations.org/local_includes/downloads/39497.pdf "));
    }
}