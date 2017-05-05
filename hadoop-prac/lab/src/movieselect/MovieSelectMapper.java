package movieselect;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieSelectMapper
  extends Mapper<LongWritable, Text, NullWritable, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
    String[] fields = value.toString().split("\\|");
    context.write(NullWritable.get(), new Text(fields[0] + " " + fields[1]));
  }
}
