package moviejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieJoinMapper
    extends Mapper<Object, Text, IntWritable, Text> {

  @Override
  public void map(Object key, Text value, Context context) {
    String[] fields = value.toString().split()
  }
}
