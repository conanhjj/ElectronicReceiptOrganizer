import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ReceiptReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private static String HOME_DIR = "/cs525/data/";

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/hadoop/conf/hdfs-site.xml"));
        FileSystem fileSystem = FileSystem.get(conf);

//        FileSystem fileSystem = new LocalFileSystem();

        Integer count = 0;
        String[] keyArr = key.toString().split(",");
        String userName = keyArr[0];
        String year = keyArr[1];
        String monthAndDay = keyArr[2];
        String dir = HOME_DIR + userName + "/" + year;
        tryCreateDir(fileSystem, dir);

        List<String> records = new LinkedList<String>();

        while(values.hasNext()) {
            Text text = values.next();
//            String[] strArr = text.toString().split(",");
//            String item = strArr[0] + " " + strArr[1];
            String item = text.toString();
            records.add(item);
            count++;
        }

        writeFile(fileSystem, dir, monthAndDay, records);
//        fileSystem.close();
        output.collect(key, new Text(count.toString()));
    }

    private void writeFile(FileSystem fs, String dir, String monthAndDay, List<String> records) {
        Path path = new Path(dir +"/" + monthAndDay);
        try {
            DataOutputStream dos = new DataOutputStream(fs.create(path));
            for(String str : records) {
                dos.writeBytes(str+"\n");
            }
            dos.flush();
            dos.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void tryCreateDir(FileSystem fs, String dir) {

        Path path = new Path(dir);
        try {
            if(fs.exists(path)) return;
            fs.mkdirs(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{
        String str =  "2013-03-19";
        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
        Date date = dateFormat.parse(str);
        System.out.println(date.toString());
        DateFormat year = new SimpleDateFormat("yyyy");
        DateFormat month = new SimpleDateFormat("mm");
        DateFormat day = new SimpleDateFormat("dd");
        System.out.println(year.format(date));
        System.out.println(month.format(date));
        System.out.println(day.format(date));
    }
}
