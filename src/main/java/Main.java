import logging.InfoLogger;
import logging.SeverLogger;

import java.io.IOException;

public class Main {
    private static SeverLogger severLogger;
    private static InfoLogger infoLogger;
    private static String s3OutputBucket = "rotemb271S3OutputBucket";

    public static void main(String[] args) throws IOException {
        severLogger = new SeverLogger("ManagerSeverLogger","severLog.txt");
        infoLogger = new InfoLogger("ManagerInfoLogger","infoLog.txt");
        infoLogger.log("Start");
        infoLogger.log("Done");
    }
}
