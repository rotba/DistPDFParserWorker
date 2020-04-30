import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;

public class VisibilityManagement implements Runnable {
    private int currentVisibilityTimeout;
    private final Message message;
    private final SqsClient sqsClient;
    private final String sqsName;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;

    public VisibilityManagement(int initialVisibilityTimeout, Message message, SqsClient sqsClient, String sqsName, InfoLogger infoLogger, SeverLogger severLogger) {
        currentVisibilityTimeout = initialVisibilityTimeout;
        this.message = message;
        this.sqsClient = sqsClient;
        this.sqsName = sqsName;
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
    }

    @Override
    public void run() {
        try {
            while (true){
                try {
                    Thread.sleep((currentVisibilityTimeout /2)*1000);
                } catch (InterruptedException e) {
                    infoLogger.log("Visibility manager interrupted and stops working");
                    return;
                }
                int increasesTo = Math.max(180, currentVisibilityTimeout * 2);
                infoLogger.log(String.format("Visibiliti manger increases visibility timout to %d", increasesTo));
                currentVisibilityTimeout = increasesTo;
                ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder()
                        .queueUrl(sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(sqsName).build()).queueUrl())
                        .build();
                sqsClient.changeMessageVisibility(changeMessageVisibilityRequest);
            }
        }catch (Exception e){
            severLogger.log("Visibility logger failed, stopping visibility manager", e);
            return;
        }
    }
}
