import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
		job.setCombinerClass(FlightDelaysCombiner.class);
		job.setReducerClass(FlightDelaysReducer.class);

		if(args.length>2){
			if(Integer.parseInt(args[2])>=0){
				job.setNumReduceTasks(Integer.parseInt(args[2]));
			}
		}
		else{
			job.setNumReduceTasks(1);
		}
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class FlightDelaysMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		private final static int DELAY = 14; //"ArrDelay";
		private final static int CARRIER = 8; // "UniqueCarrier";
		private final static int MONTH = 1; //"Month";
		private final static IntWritable month = new IntWritable();
		private final static Text data = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			
			if (key.get() > 0) {
				String originalDelay = tokens[DELAY];
				
				int delay = 0;
				if(StringUtils.isNumeric(originalDelay)) {
					delay = Integer.parseInt(originalDelay);
				}

				month.set(Integer.parseInt(tokens[MONTH]));
				data.set(tokens[CARRIER] + "," + delay);
				context.write(month, data);
			}
		}			
	}
	
	public static class FlightDelaysReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

		public static Map<String,Integer> accumulateDelays(Iterable<Text> values) {

			Map<String,Integer> map = new HashMap<String,Integer>();
			
			for(Text t: values) {
				String[] tokens = t.toString().split(",");
				String carrier = tokens[0];
				int delay = Integer.parseInt(tokens[1]);
				int currentDelay = 0;
				
				if(map.containsKey(carrier)) {
					currentDelay = map.get(carrier);
				}

				map.put(carrier, delay + currentDelay);
			}
			
			return map;
		}
		
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {

			Map<String,Integer> map = accumulateDelays(values);
			
			List<Entry<String, Integer>> entryList = new ArrayList<Entry<String, Integer>>(map.entrySet());
			Collections.sort(entryList, new Comparator<Entry<String, Integer>>(){
			    @Override
			    public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
			        return e2.getValue() - e1.getValue(); // descending order
			    }
			});
			
			int idx = entryList.size() < 3 ? entryList.size() : 3;
			for(int i = 0; i < idx; i++) {
				context.write(key, new Text(entryList.get(i).getKey()));
			}
		}
	}
	
	public static class FlightDelaysCombiner extends Reducer<IntWritable,Text,IntWritable,Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			Map<String,Integer> map = FlightDelaysReducer.accumulateDelays(values);
			for(String k : map.keySet()) {
				context.write(key, new Text(k + "," + map.get(k)));
			}
		}
		
	}
}