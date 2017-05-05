package moviegroupby;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieGroupByMapper
    extends Mapper<Object, Text, IntWritable, IntWritable> {

  private static final IntWritable one = new IntWritable(1);

  @Override
  public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
    String[] fields = value.toString().split("[\\s+\\t+]");
    context.write(new IntWritable(Integer.parseInt(fields[2])), one);
  }
}