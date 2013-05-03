import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.FileInputStream;

public class MainEntry {

    public static void main1(String[] args) {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(LineIndexer.class);

        conf.setJobName("LineIndexer");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path("/cs525/input"));
        FileOutputFormat.setOutputPath(conf, new Path("/cs525/output"));

        conf.setMapperClass(LineIndexer.class);
        conf.setReducerClass(LineIndexerReducer.class);

        client.setConf(conf);

        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        rebuildReceipt();
        buildIndex();

    }

    private static void buildIndex() {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(ReceiptMapper.class);

        conf.setJobName("BuildIndex");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path("/cs525/input"));
        FileOutputFormat.setOutputPath(conf, new Path("/cs525/output2"));

        conf.setMapperClass(IndexMapper.class);
        conf.setReducerClass(IndexReducer.class);

        client.setConf(conf);

        try {
            JobClient.runJob(conf);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void rebuildReceipt() {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(ReceiptMapper.class);

        conf.setJobName("RebuildReceipt");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path("/cs525/input"));
        FileOutputFormat.setOutputPath(conf, new Path("/cs525/output"));

        conf.setMapperClass(ReceiptMapper.class);
        conf.setReducerClass(ReceiptReducer.class);

        client.setConf(conf);

        try {
            JobClient.runJob(conf);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
