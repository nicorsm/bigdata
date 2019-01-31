import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLengthCount {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length < 2) {
      System.out.printf("Usage: WordCount <input dir> <output dir> [<numReduceTask>]\n");
      System.exit(-1);
    }

    /*
     * Instantiate a Job object for your job's configuration. 
     */
	Job job = Job.getInstance(new Configuration(), "Word Length Count");
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(WordLengthCount.class);

    job.setMapperClass(WordLengthCountMapper.class);
    job.setCombinerClass(WordLengthCountReducer.class);
    job.setReducerClass(WordLengthCountReducer.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    if(args.length > 2) {
    	int numReduceTasks = Integer.parseInt(args[2]);
    	if(numReduceTasks >= 0) {
        	job.setNumReduceTasks(numReduceTasks);
    	}
    } else { 
    	job.setNumReduceTasks(1);
    }
    
    //Sets the classes for output data (keys = text, values = integers)
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

