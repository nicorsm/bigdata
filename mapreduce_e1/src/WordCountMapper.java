import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		//Input phase: splitting strings into words
		
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			
			//Mapping phase: associating number of single occurrencies to each word (e.g. "sopra", 1) 
			context.write(word, one);
		}
	}
}
