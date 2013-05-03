import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class QueryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>  {

    private static String TAG;
    private static String USERNAME;

    private static final Text mapperKey = new Text();
    private static final Text mapperValue = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] strArr = line.split(",");
        String tag = strArr[0];
        if(!tag.equals(TAG)) {
            return;
        }


        StringBuilder sb = new StringBuilder();
        for(int i=1; i<strArr.length;++i) {
            if(i == 1) {
                sb.append(strArr[i]);
            } else {
                sb.append(",");
                sb.append(strArr[i]);
            }
        }
        mapperKey.set(USERNAME + "," + TAG);
        mapperValue.set(sb.toString());
        output.collect(mapperKey, mapperValue);
    }

    public static void setTAG(String newTag) {
        TAG = newTag;
    }

    public static void setUSERNAME(String userName) {
        USERNAME = userName;
    }
}
