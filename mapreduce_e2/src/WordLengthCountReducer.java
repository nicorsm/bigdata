import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordLengthCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private IntWritable result = new IntWritable();
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		
		//Reduce phase: sums the values in arrays (e.g., "10", [1,1,1,1] => "10", 4)
		for(IntWritable value: values) {
			sum += value.get();
		}
		
		result.set(sum);
		
		context.write(key, result);
	}
}