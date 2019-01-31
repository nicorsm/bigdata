import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordLengthCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private IntWritable wordLength = new IntWritable();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		
		//Input phase: splitting strings into words
		
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			wordLength.set(word.getLength());
			//Mapping phase: associating 1 to each word length (e.g. "sopra" -> 5, 1) 
			context.write(wordLength, one);
		}
	}
}
