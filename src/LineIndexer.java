import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.StringTokenizer;

public class LineIndexer extends MapReduceBase implements Mapper<LongWritable, Text, Text ,Text> {

    private final static Text word = new Text();
    private final static Text location = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        location.set(fileName);

        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line.toLowerCase());
        while(itr.hasMoreElements()) {
            word.set(itr.nextToken());
            output.collect(word, location);
        }
    }
}
