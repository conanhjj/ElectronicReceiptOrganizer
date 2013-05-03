import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class IndexReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private static String HOME_DIR = "/cs525/data/";

    private Map<String, List<String>> tagMap = new HashMap<String, List<String>>();

    @Override
    public void reduce(Text keys, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/hadoop/conf/hdfs-site.xml"));
        FileSystem fileSystem = FileSystem.get(conf);
        String userName = keys.toString();
        String dir = HOME_DIR + userName + "/index";

        OutputStream os = tryCreateIndexFile(fileSystem,  dir);
        if(os == null) return;
        DataOutputStream dos = new DataOutputStream(os);

        while(values.hasNext()) {
            Text text = values.next();
            String[] strArr = text.toString().split(",");
            String tag = strArr[0];
            String date = strArr[1];
            if(tagMap.containsKey(tag)) {
                List<String> list = tagMap.get(tag);
                if(!list.contains(date)) {
                    list.add(date);
                }
            } else {
                List<String> list = new LinkedList<String>();
                list.add(date);
                tagMap.put(tag, list);
            }
        }

        for(Map.Entry<String, List<String>> entry : tagMap.entrySet()) {
            String key = entry.getKey();
            List<String> list = entry.getValue();
            dos.writeBytes(key);
            for(String str : list) {
                dos.writeBytes(","+str);
            }
            dos.writeBytes("\n");
        }
        dos.close();
    }

    private OutputStream tryCreateIndexFile(FileSystem fs, String dir) {
        Path path = new Path(dir);
        try {
            return fs.create(path);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
