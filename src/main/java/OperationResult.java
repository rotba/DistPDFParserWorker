public class OperationResult {
    private final String outputPath;
    private final String inputPath;

    public OperationResult(String outputPath, String inputPath) {
        this.outputPath = outputPath;
        this.inputPath = inputPath;
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
