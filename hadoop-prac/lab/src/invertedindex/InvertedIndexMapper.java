package invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper
    extends Mapper<LongWritable, Text, Text, Text> {

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
    String cleanedLine = value.toString().replace("[^a-zA-Z\\s]", "");
    StringTokenizer st = new StringTokenizer(cleanedLine);
    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    while (st.hasMoreTokens()) {
      context.write(new Text(st.nextToken()), new Text(fileName));
    }
  }
}
