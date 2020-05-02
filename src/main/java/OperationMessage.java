import org.apache.commons.cli.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.lang.invoke.WrongMethodTypeException;

class OperationMessage {
    private final Message message;
    private CommandLine operation;
    private Action action;

    public OperationMessage(Message m) throws ParseException, UnfamiliarActionException {
        this.message = m;
        Options operationParsingOptions = new Options();
        Option action = new Option("a", "action", true, "action");
        action.setRequired(true);
        operationParsingOptions.addOption(action);
        Option input = new Option("i", "input", true, "input file");
        input.setRequired(true);
        operationParsingOptions.addOption(input);
        Option outBucket = new Option("b", "outputBucket", true, "output bucket");
        outBucket.setRequired(true);
        operationParsingOptions.addOption(outBucket);
        Option outBucketKey = new Option("k", "outputBucketKey", true, "output bucket key");
        outBucketKey.setRequired(true);
        operationParsingOptions.addOption(outBucketKey);
        Option timeStamp = new Option("t", "timestamp", true, "unique timestamp");
        timeStamp.setRequired(true);
        operationParsingOptions.addOption(timeStamp);
        CommandLineParser operationParser = new DefaultParser();
        operation = operationParser.parse(operationParsingOptions, message.body().split("\\s+"));
        this.action = parseAction();
    }

    private Action parseAction() throws UnfamiliarActionException {
        if (operation.getOptionValue("a").equals("ToImage"))
            return new ToImage();
        if (operation.getOptionValue("a").equals("ToHTML"))
            return new ToHTML();
        if (operation.getOptionValue("a").equals("ToText"))
            return new ToText();
        if (operation.getOptionValue("a").equals("FORTESTING"))
            return new FORTESTING();
        throw new UnfamiliarActionException(operation.getOptionValue("a"));
    }

    public Action getAction(){
        return action;
    }
    public String getActionString(){
        return operation.getOptionValue("a");
    }
    public String getInput() {
        return operation.getOptionValue("i");
    }

    public String getBucket() {
        return operation.getOptionValue("b");
    }

    public String getKey() {
        return operation.getOptionValue("k");
    }
    public String getTimeStamp() {
        return operation.getOptionValue("t");
    }

    public Message getMessage() {
        return message;
    }
    abstract class Action{
        public abstract OperationResult visit(Worker worker) throws Worker.NotImplementedException;

        public  String getInput(){
            return OperationMessage.this.getInput();
        }

        public String getKey() {
            return OperationMessage.this.getKey();
        }
    }

    class ToImage extends Action{
        @Override
        public OperationResult visit(Worker worker) throws Worker.NotImplementedException{
            return worker.accept(this);
        }
    }

    class ToHTML extends Action {
        @Override
        public OperationResult visit(Worker worker) throws Worker.NotImplementedException{
            return worker.accept(this);
        }
    }

    class ToText extends Action {
        @Override
        public OperationResult visit(Worker worker) throws Worker.NotImplementedException{
            return worker.accept(this);
        }
    }

    class FORTESTING extends Action {
        @Override
        public OperationResult visit(Worker worker) throws Worker.NotImplementedException{
            return worker.accept(this);
        }
    }
    public class UnfamiliarActionException extends Exception {
        public UnfamiliarActionException(String message) {
            super(message);
        }

    }
}
