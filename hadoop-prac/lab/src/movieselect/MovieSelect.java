package movieselect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// movie id|movie title|release data|video release date|IMDB url
// SELECT movie_id, movie_title from
// mapper output:  offset (LongWritable), selected columns
public class MovieSelect {
  public static void main(String[] args)
    throws IOException, ClassNotFoundException,InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "movie select");

    job.setJarByClass(MovieSelect.class);
    job.setMapperClass(MovieSelectMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);

    // here to set no reduce
    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true)? 0 : 1);
  }
}
