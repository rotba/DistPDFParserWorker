import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import org.apache.commons.cli.*;

public class Main {
    private static SeverLogger severLogger;
    private static InfoLogger infoLogger;
    public static Region region = Region.US_EAST_1;
    public final static int initialVisibilityTimoutSecs = 30;

    public static void main(String[] args) throws IOException {
        severLogger = new SeverLogger("WorkerSeverLogger","severLog.txt");
        infoLogger = new InfoLogger("WorkerInfoLogger","infoLog.txt");
        infoLogger.log("Start");
        Options options = new Options();
        Option notificationsOutputSqs = new Option("n", "notifications", true, "notifications sqs");
        notificationsOutputSqs.setRequired(true);
        options.addOption(notificationsOutputSqs);
        Option operationsSqs = new Option("o", "operations", true, "operations sqs");
        operationsSqs.setRequired(true);
        options.addOption(operationsSqs);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            severLogger.log("Failed parsing args", e);
            infoLogger.log("Exisiting");
            System.exit(1);
        }
        try {
            new Worker(
                    cmd.getOptionValue("n"),
                    cmd.getOptionValue("o"),
                    Main.region,
                    initialVisibilityTimoutSecs,
                    infoLogger,
                    severLogger
            ).work();
        }catch (Exception e){
            severLogger.log("Worker failed", e);
        }
        infoLogger.log("Done");
    }
}
