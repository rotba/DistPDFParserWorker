public class Utils {
    public static void waitParsingTime() {
        try {
            Thread.sleep(10*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
