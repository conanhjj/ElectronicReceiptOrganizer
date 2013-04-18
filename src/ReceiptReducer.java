import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;

public class ReceiptReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/hadoop/conf/hadoop-site.xml"));
        
        Float sum = 0f;

        String userId = key.toString();
        String receiptId = values.next().toString();
        String date = values.next().toString();

        while(values.hasNext()) {
            Float value = Float.valueOf(values.next().toString());
            sum += value;
        }

        output.collect(key, new Text(sum.toString()));
    }

    public static void main(String[] args) throws Exception{
        Scanner in = new Scanner(new FileInputStream("receipt-joined.txt"));
        while(in.hasNext()) {
            System.out.println(in.nextLine());
        }
    }
}
