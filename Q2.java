import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q2 {

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



    public static class DiameterReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double[] x = new double[1000000];
			double[] y = new double[1000000];
       		double diameter = 0.0;
			
			int i = 0;
            for (Text val: values) {
				String[] coordinate = val.toString().split(" ");
				x[i] = Double.valueOf(coordinate[0]);
				y[i] = Double.valueOf(coordinate[1]);
				i++;
			}

			int n = i;
			for (i = 0; i < n; i++) {
				for (int j = 0; j < i; j++) {
					double dist = Math.sqrt((x[i] - x[j]) * (x[i] - x[j]) + (y[i] - y[j]) * (y[i] - y[j]));
					if (dist > diameter) {
						diameter = dist;
					}
				}
			}
			
			result.set(diameter);
			context.write(key, result);
        }
  }



  	public static void main(String[] args) throws Exception {

    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Q2");
    	job.setJarByClass(Q2.class);
    	job.setMapperClass(POIMapper.class);
    	//job.setCombinerClass(DiameterReducer.class);
    	job.setReducerClass(DiameterReducer.class);
    	job.setOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


