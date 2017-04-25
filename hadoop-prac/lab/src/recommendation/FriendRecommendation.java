package recommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class FriendRecommendation {

  public static class CommonFriendMapper
      extends Mapper<Object, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private final static Text tuple = new Text();

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] ids = value.toString().split("\\t+|,");
      for (int i = 1; i < ids.length; i++) {
        for (int j = 1; j < ids.length; j++) {
          if (i == j)
            continue;
          tuple.set(ids[i] + "," + ids[j]);
          context.write(tuple, one);
        }
      }
    }
  }

  public static class CommonFriendReducer
      extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final static LongWritable result = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<LongWritable> value, Context context)
      throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable one : value)
        sum += one.get();
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class TopTenMapper
    extends Mapper<Object, Text, Text, Text> {

    private Text person  = new Text();
    private Text info = new Text();

    @Override
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String[] items = value.toString().split(",|\\s+");
      person.set(items[0]);
      info.set(items[1] + " " + items[2]);
      context.write(person, info);
    }
  }

  public static class TopTenReducer
    extends Reducer<Text, Text, Text, Text> {

    private Text list = new Text();

    @Override
    public void reduce(Text person, Iterable<Text> info, Context context)
      throws IOException, InterruptedException {
      PriorityQueue<String> pq = new PriorityQueue<>(11, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          long num1 = Long.parseLong(o1.split("\\s+|\\t+")[1]);
          long num2 = Long.parseLong(o2.split("\\s+|\\t+")[1]);
          if (num1 == num2)
            return 0;
          else if (num1 < num2)
            return 1;
          else
            return -1;
        }
      });
      for (Text t : info) {
        pq.add(t.toString());
        if (pq.size() == 11)
          pq.poll();
      }
      String recom = "";
      while (!pq.isEmpty()) {
        recom += pq.poll().split("\\s+")[0];
      }
      list.set(recom);
      context.write(person, list);
    }
  }

  public static boolean jobFirst(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "friend recommendation");
    job.setJarByClass(FriendRecommendation.class);
    job.setMapperClass(CommonFriendMapper.class);
    job.setCombinerClass(CommonFriendReducer.class);
    job.setReducerClass(CommonFriendReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true);
  }

  public static boolean jobSecond(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "top 10 friend");
    job.setJarByClass(FriendRecommendation.class);
    job.setMapperClass(TopTenMapper.class);
    job.setCombinerClass(TopTenReducer.class);
    job.setReducerClass(TopTenReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    return job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
    if (jobFirst(args)) {
      System.exit(jobSecond(args)? 0 : 1);
    }

  }

}

