import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightDelaysAnalyzer {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Flight Delays Analyzer");
		job.setJarByClass(FlightDelaysAnalyzer.class);
		
		job.setMapperClass(FlightDelaysMapper.class);
		job.setCombinerClass(FlightDelaysReducer.class);
		job.setReducerClass(FlightDelaysReducer.class);

		if(args.length>2){
			if(Integer.parseInt(args[2])>=0){
				job.setNumReduceTasks(Integer.parseInt(args[2]));
			}
		}
		else{
			job.setNumReduceTasks(1);
		}
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class FlightDelaysMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static int DELAY = 14; //"ArrDelay";
		private final static int CARRIER = 8; // "UniqueCarrier";
		private final static int MONTH = 1; //"Month";
		private final static IntWritable month = new IntWritable();
		private final static Text carrier = new Text();
		private final static Text monthCarrier = new Text();
		private final static IntWritable delay = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			
			if (key.get() > 0) {
				String originalDelay = tokens[DELAY];
				
				if(StringUtils.isNumeric(originalDelay)) {
					delay.set(Integer.parseInt(originalDelay));
				} else {
					delay.set(0);
				}

				int month = Integer.parseInt(tokens[MONTH]);
				
				monthCarrier.set(String.format("%02d,%s", month, tokens[CARRIER]));
				context.write(monthCarrier, delay);
			}
		}			
	}
	
	public static class FlightDelaysReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}