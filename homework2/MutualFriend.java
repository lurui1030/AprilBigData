import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yuyu on 4/25/17.
 */
public class MutualFriend {
    public static class MutualFriendMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            if(value == null) {
                return;
            }
            String source = value.toString().trim();
            if (source.length() == 0) {
                return;
            }
            String[] userIdPlusFriends = source.split("\\s+");
            if (userIdPlusFriends.length < 2) {
                return;
            }
            String[] friends = userIdPlusFriends[1].split(",");
            for (int i = 0; i < friends.length; i++) {
                for (int j = 0; j < friends.length; j++) {
                    if (i == j) {
                        continue;
                    }
                    StringBuilder mutualFriend = new StringBuilder();
                    mutualFriend.append(friends[i]).append("-").append(friends[j]);
                    context.write(new Text(mutualFriend.toString()), new IntWritable(1));
                }
            }
        }
    }
    public static class MutualFriendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter", "\n");
        Job job = Job.getInstance(conf, "MutualFriend");
        job.setJarByClass(MutualFriend.class);
        job.setMapperClass(MutualFriendMapper.class);
        job.setReducerClass(MutualFriendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
