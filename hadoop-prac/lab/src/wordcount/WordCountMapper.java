package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper
    extends Mapper<IntWritable, Text, Text, IntWritable> {
  @Override
  public void map(IntWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String cleanedLine = value.toString().replaceAll("[^a-zA-Z\\s]", "");
    StringTokenizer st = new StringTokenizer(cleanedLine);
    while (st.hasMoreTokens()) {
      context.write(new Text(st.nextToken()), new IntWritable(1));
    }
  }
}
