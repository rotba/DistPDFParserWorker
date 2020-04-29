import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import org.apache.commons.cli.*;

public class Main {
    private static SeverLogger severLogger;
    private static InfoLogger infoLogger;
    public static Region region = Region.US_EAST_1;

    public static void main(String[] args) throws IOException {
        severLogger = new SeverLogger("WorkerSeverLogger","severLog.txt");
        infoLogger = new InfoLogger("WorkerInfoLogger","infoLog.txt");
        infoLogger.log("Start");
        Options options = new Options();
        Option action = new Option("a", "action", true, "action");
        action.setRequired(true);
        options.addOption(action);
        Option input = new Option("i", "input", true, "input file");
        input.setRequired(true);
        options.addOption(input);
        Option notificationsOutputSqs = new Option("o", "output", true, "notifications output sqs");
        notificationsOutputSqs.setRequired(true);
        options.addOption(notificationsOutputSqs);
        Option outBucket = new Option("b", "outputBucket", true, "output bucket");
        outBucket.setRequired(true);
        options.addOption(outBucket);
        Option outBucketKey = new Option("k", "outputBucketKey", true, "output bucket key");
        outBucketKey.setRequired(true);
        options.addOption(outBucketKey);
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
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
                    cmd.getOptionValue("a"),
                    cmd.getOptionValue("i"),
                    cmd.getOptionValue("o"),
                    cmd.getOptionValue("b"),
                    cmd.getOptionValue("k"),
                    Main.region,
                    infoLogger,
                    severLogger
            ).work();
        }catch (Exception e){
            severLogger.log("Worker failed", e);
        }
        infoLogger.log("Done");
    }
}
