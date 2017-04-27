package marlabwordcount;
import java.io.IOException;
import java.util.StringTokenizer;

/*
 * MapInputKey: line number, IntWritable
 * MapInputValue:the content for the line, text
 * 
 * MapOutputKey:word, text
 * MapOutputValue:1, IntWritable(hadoop中的int)
 * 
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class wordcountmapper extends Mapper<IntWritable, Text, Text, IntWritable>{
	public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException{
		String cleaned_line = value.toString().replaceAll("(^a-zA-Z\\s)", "");
		StringTokenizer st = new StringTokenizer(cleaned_line);// StringTokenizer就像一个迭代器
		while(st.hasMoreTokens()) {
			context.write(new Text(st.nextToken()), new IntWritable(1));
		}
	}
}
