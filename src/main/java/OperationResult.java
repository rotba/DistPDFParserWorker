public class OperationResult {
    private final String outputPath;
    private final String inputPath;
    private final String finalBucket;
    private final String finalKey;

    public String getFinalBucket() {
        return finalBucket;
    }

    public String getFinalKey() {
        return finalKey;
    }

    public OperationResult(String outputPath, String inputPath, String finalBucket, String finalKey) {
        this.outputPath = outputPath;
        this.inputPath = inputPath;
        this.finalBucket = finalBucket;
        this.finalKey = finalKey;
    }

    public boolean isSuccess() {
        return true;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getDesc() {
        return "success";
    }
}
