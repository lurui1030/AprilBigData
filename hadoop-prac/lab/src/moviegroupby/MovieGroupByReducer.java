package moviegroupby;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieGroupByReducer
    extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

  @Override
  public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
    int num = 0;
    for (IntWritable value : values)
      num += value.get();
    context.write(key, new IntWritable(num));
  }
}
