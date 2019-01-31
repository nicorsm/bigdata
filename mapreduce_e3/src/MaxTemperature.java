import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.thriftfs.api.IOException;

public class MaxTemperature {

  public static void main(String[] args) throws IOException {
	  JobConf conf = new JobConf(MaxTemperature.class);
	  conf.setJobName("Max Temperature");
	  
	  Path input = new Path(args[0]), output = new Path(args[1]);
	  FileSystem fs = FileSystem.get(new Configuration());
	  if(fs.exists(output)) {
		  fs.delete(output, true);
	  }

    FileInputFormat.addInputPath(conf, input);
    FileOutputFormat.setOutputPath(conf, output);
    
    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setReducerClass(MaxTemperatureReducer.class);
    conf.setCombinerClass(MaxTemperatureCombiner.class);
    
    if(args.length > 2 && Integer.parseInt(args[2]) >= 0) {
    	conf.setNumReduceTasks(Integer.parseInt(args[2]));
    } else {
    	conf.setNumReduceTasks(1);
    }
    
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(DoubleWritable.class);
    
    JobClient.runJob(conf);
  }
}

