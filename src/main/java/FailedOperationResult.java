public class FailedOperationResult extends OperationResult {
    private final String operationNotImpleneted;
    private final Throwable throwable;

    public FailedOperationResult(String operationNotImpleneted, Throwable throwable, String finalBucket,String finalKey ) {
        super(null,null,finalBucket,finalKey);
        this.operationNotImpleneted = operationNotImpleneted;
        this.throwable = throwable;
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
