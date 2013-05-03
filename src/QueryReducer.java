import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.*;
import java.util.Iterator;
import java.util.Scanner;

public class QueryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>  {

    private static String HOME_DIR = "/cs525/data/";

    private static StringBuilder result;

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/hadoop/conf/hdfs-site.xml"));
        FileSystem fileSystem = FileSystem.get(conf);

        String userName = key.toString().split(",")[0];
        String tag = key.toString().split(",")[1];
        result = new StringBuilder();

        while(values.hasNext()) {
            String value = values.next().toString();
            String[] strArr = value.split(",");
            for(String str : strArr) {
                String year = str.substring(0, 4);
                String monthAndDay = str.substring(5);
                loadFile(fileSystem, HOME_DIR + userName + "/" + year + "/" + monthAndDay, tag, str);
            }
        }
    }

    public void loadFile(FileSystem fs, String dir, String tag, String date) {
        Path path = new Path(dir);
        try {
            Scanner scanner = new Scanner(fs.open(path));
            String str = scanner.nextLine();
            Receipt receipt = Receipt.loadReceipt(str.split(","));
            receipt.receiptDate = date;

            if(receipt.tag.equals(tag)) {
                result.append(receipt.toString()).append("\n");
            }
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getResult() {
        if(result == null) return "";
        return result.toString();
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        String str = "Meat";
        String newStr = new String(str.getBytes("UTF-8"));
        System.out.println(str.length());
        System.out.println(newStr.length());
        System.out.println(str.equals(newStr));
    }
}
