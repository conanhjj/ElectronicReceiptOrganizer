import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class IndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private static final Text mapperKey = new Text();
    private static final Text mapperValue = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] strArr = line.split(",");
        Receipt receipt = Receipt.loadRawReceipt(strArr);

        StringBuilder sb = new StringBuilder();
        sb.append(receipt.userName);
        mapperKey.set(sb.toString());

        sb = new StringBuilder();
        sb.append(receipt.tag);
        sb.append(",");
        sb.append(receipt.receiptDate);
        mapperValue.set(sb.toString());

        output.collect(mapperKey, mapperValue);
    }
}
