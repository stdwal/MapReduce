import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {

    public static class POIMapper extends Mapper<Object, Text, Text, Text>{

        private Text keyword = new Text();
        private Text coordinate = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ", 2);
            keyword.set(tokens[0]);
            coordinate.set(tokens[1]);
            context.write(keyword, coordinate);
        }
    }

    public static class CentroidReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumX = 0.0;
            double sumY = 0.0;
            int cnt = 0;
            for (Text val : values) {
                String[] coordinate = val.toString().split(" ");
                sumX += Double.valueOf(coordinate[0]);
                sumY += Double.valueOf(coordinate[1]);
                cnt++;
            }
            double centroidX = sumX / cnt;
            double centroidY = sumY / cnt;
            String s = centroidX + " " + centroidY;
            result.set(s);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
  	    Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Q1");
    	job.setJarByClass(Q1.class);
    	job.setMapperClass(POIMapper.class);
    	job.setCombinerClass(CentroidReducer.class);
    	job.setReducerClass(CentroidReducer.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}

