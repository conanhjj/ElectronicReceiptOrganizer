import java.io.FileInputStream;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: Wind
 * Date: 3/31/13
 * Time: 8:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestClass {

    private static String receiptLineId;
    private static String receiptId;
    private static String userId;
    private static String name;
    private static String store;
    private static String unitPrice;
    private static String subTotalPrice;
    private static String quantity;
    private static String receiptDate;
    private static String tag;

    public static void main(String[] args) throws Exception{
        Scanner scanner = new Scanner(new FileInputStream("receipt-joined.txt"));
        while(scanner.hasNext()) {
            String str = scanner.nextLine();
            loadReceipt(str.split(","));
            System.out.println(receiptLineId + ", " + receiptId + ", " + unitPrice);
        }
    }

    private static void loadReceipt(String[] strArr) {
        for(int i=0;i<strArr.length;++i) {
            if(strArr[i].startsWith("\"") && strArr[i].endsWith("\"")) {
                strArr[i] = strArr[i].substring(1, strArr[i].length()-1);
            }
        }
        receiptLineId = strArr[0];
        receiptId = strArr[1];
        userId = strArr[2];
        name = strArr[3];
        store = strArr[4];
        unitPrice = strArr[5];
        quantity = strArr[6];
        subTotalPrice = strArr[7];
        receiptDate = strArr[8];
        tag = strArr[9];
    }
}
