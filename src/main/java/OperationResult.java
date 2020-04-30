import java.nio.file.Path;

public class OperationResult {
    private final String toPath;

    public OperationResult(String toPath) {
        this.toPath = toPath;
    }

    public boolean isSuccess() {
        return true;
    }

    public String filePath() {
        return toPath;
    }

    public String getDesc() {
        return "success";
    }
}
