public class Utils {
    public static void waitParsingTime() {
        try {
            Thread.sleep(15*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
