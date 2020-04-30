public class FailedOperationResult extends OperationResult {
    private final String operationNotImpleneted;

    public FailedOperationResult(String operationNotImpleneted) {
        super(null);
        this.operationNotImpleneted = operationNotImpleneted;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public String getDesc() {
        return "\""+operationNotImpleneted+"\"";
    }
}
