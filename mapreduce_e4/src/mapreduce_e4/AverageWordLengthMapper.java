package mapreduce_e4;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageWordLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text word = new Text(), firstLetter = new Text();
	private IntWritable wordLength = new IntWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			firstLetter.set(word.toString().substring(0, 1));
			wordLength.set(word.getLength());
			context.write(firstLetter, wordLength);
		}
	}
}