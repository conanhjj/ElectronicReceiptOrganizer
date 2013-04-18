import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;

public class ReceiptMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private String receiptLineId;
    private String receiptId;
    private String userId;
    private String name;
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

        StringBuilder sb = new StringBuilder();
        sb.append(userId);
//        sb.append(" ");
//        sb.append(tag);
        mapperKey.set(sb.toString());

        sb = new StringBuilder();
        sb.append(unitPrice);
        sb.append(" ");
        sb.append(receiptDate);
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
        userId = strArr[2];
        name = strArr[3];
        store = strArr[4];
        unitPrice = strArr[5];
        quantity = strArr[6];
        subTotalPrice = strArr[7];
        receiptDate = strArr[8];
        tag = strArr[9];

        Integer i = 5;
        int[] count = new int[10];
        count[i] = 2;
    }

}
