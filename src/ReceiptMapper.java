import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class ReceiptMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private static final Text mapperKey = new Text();
    private static final Text mapperValue = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        Receipt receipt;

        String line = value.toString();
        String[] strArr = line.split(",");
        receipt = Receipt.loadRawReceipt(strArr);
        String year = receipt.receiptDate.substring(0, 4);
        String monthAndDay = receipt.receiptDate.substring(5);

        StringBuilder sb = new StringBuilder();
        sb.append(receipt.userName);
        sb.append(",");
        sb.append(year);
        sb.append(",");
        sb.append(monthAndDay);
//        sb.append(tag);
        mapperKey.set(sb.toString());


        sb = new StringBuilder();
        sb.append(receipt.itemName);
        sb.append(",");
        sb.append(receipt.store);
        sb.append(",");
        sb.append(receipt.unitPrice);
        sb.append(",");
        sb.append(receipt.quantity);
        sb.append(",");
        sb.append(receipt.subTotalPrice);
        sb.append(",");
        sb.append(receipt.tag);
        mapperValue.set(sb.toString());

        output.collect(mapperKey, mapperValue);
    }
}
