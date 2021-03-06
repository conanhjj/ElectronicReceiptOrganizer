
public class Receipt {

    public String receiptLineId;
    public String receiptId;
    public String userName;
    public String itemName;
    public String store;
    public String unitPrice;
    public String quantity;
    public String subTotalPrice;
    public String receiptDate;
    public String tag;

    public Receipt() {

    }

    public static Receipt loadRawReceipt(String[] strArr) {
        Receipt receipt = new Receipt();
        for(int i=0;i<strArr.length;++i) {
            if(strArr[i].startsWith("\"") && strArr[i].endsWith("\"")) {
                strArr[i] = strArr[i].substring(1, strArr[i].length()-1);
            }
        }
        receipt.receiptLineId = strArr[0];
        receipt.receiptId = strArr[1];
        receipt.userName = strArr[2];
        receipt.itemName = strArr[3];
        receipt.store = strArr[4];
        receipt.unitPrice = strArr[5];
        receipt.quantity = strArr[6];
        receipt.subTotalPrice = strArr[7];
        receipt.receiptDate = strArr[8];
        receipt.tag = strArr[9];

        return receipt;
    }

    public static Receipt loadReceipt(String[] strArr) {
        Receipt receipt = new Receipt();
        for(int i=0;i<strArr.length;++i) {
            if(strArr[i].startsWith("\"") && strArr[i].endsWith("\"")) {
                strArr[i] = strArr[i].substring(1, strArr[i].length()-1);
            }
        }
        receipt.itemName = strArr[0];
        receipt.store = strArr[1];
        receipt.unitPrice = strArr[2];
        receipt.quantity = strArr[3];
        receipt.subTotalPrice = strArr[4];
        receipt.tag = strArr[5];

        return receipt;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        if(userName != null) {
            sb.append(userName).append(" ");
        }
        sb.append(itemName).append(" ");
        sb.append(store).append(" ");
        sb.append(unitPrice).append(" ");
        sb.append(quantity).append(" ");
        sb.append(subTotalPrice).append(" ");
        sb.append(receiptDate).append(" ");
        sb.append(tag).append(" ");
        return sb.toString();
    }
}
