import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class ReceiptMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private String receiptLineId;
    private String receiptId;
    private String userName;
    private String itemName;
    private String store;
    private String unitPrice;
    private String quantity;
    private String subTotalPrice;
    private String receiptDate;
    private String tag;

    private static final Text mapperKey = new Text();
    private static final Text mapperValue = new Text();


    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String line = value.toString();
        String[] strArr = line.split(",");
        loadReceipt(strArr);
        String year = receiptDate.substring(0, 4);
        String monthAndDay = receiptDate.substring(5);

        StringBuilder sb = new StringBuilder();
        sb.append(userName);
        sb.append(",");
        sb.append(year);
        sb.append(",");
        sb.append(monthAndDay);
//        sb.append(tag);
        mapperKey.set(sb.toString());


        sb = new StringBuilder();
        sb.append(itemName);
        sb.append(",");
        sb.append(store);
        sb.append(",");
        sb.append(unitPrice);
        sb.append(",");
        sb.append(quantity);
        sb.append(",");
        sb.append(subTotalPrice);
        sb.append(",");
        sb.append(tag);
        mapperValue.set(sb.toString());

        output.collect(mapperKey, mapperValue);
    }

    private void loadReceipt(String[] strArr) {
        for(int i=0;i<strArr.length;++i) {
            if(strArr[i].startsWith("\"") && strArr[i].endsWith("\"")) {
                strArr[i] = strArr[i].substring(1, strArr[i].length()-1);
            }
        }
        receiptLineId = strArr[0];
        receiptId = strArr[1];
        userName = strArr[2];
        itemName = strArr[3];
        store = strArr[4];
        unitPrice = strArr[5];
        quantity = strArr[6];
        subTotalPrice = strArr[7];
        receiptDate = strArr[8];
        tag = strArr[9];
    }

}
