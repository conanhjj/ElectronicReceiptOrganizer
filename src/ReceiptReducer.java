import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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

    private static String HOME_DIR = "/home/jhu14/cs525/data/";

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

//        Configuration conf = new Configuration();
//        FileSystem fileSystem = FileSystem.get(conf);
//        conf.addResource(new Path("/hadoop/conf/core-site.xml"));
//        conf.addResource(new Path("/hadoop/conf/hadoop-site.xml"));

        FileSystem fileSystem = new LocalFileSystem();

        Integer count = 0;
        String userId = key.toString();
        String receiptId, date;

        while(values.hasNext()) {
            Text text = values.next();
            String[] strArr = text.toString().split(" ");
            receiptId = strArr[0];
            date = strArr[1];
            createDir(fileSystem, userId, receiptId, date);
//            Path path = new Path(HOME_DIR + userId )
            count++;
        }

        fileSystem.close();
        output.collect(key, new Text(count.toString()));
    }

    private void createDir(FileSystem fs, String userId, String receiptId, String date) {
        Path path = new Path(HOME_DIR + userId + "/" + receiptId);
        try {
            if(fs.exists(path)) return;
            fs.mkdirs(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{
        Scanner in = new Scanner(new FileInputStream("receipt-joined.txt"));
        while(in.hasNext()) {
            System.out.println(in.nextLine());
        }
    }
}