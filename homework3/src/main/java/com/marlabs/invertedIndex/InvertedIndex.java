package com.marlabs.invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

/**
 * Created by yuyu on 4/26/17.
 */
public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            if (value == null) {
                return;
            }
            String source = value.toString().toLowerCase();
            source = source.replaceAll("[^a-z]", " ").trim();
            if (source.length() == 0) {
                return;
            }
            StringTokenizer st = new StringTokenizer(source);
            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            while (st.hasMoreTokens()) {
                context.write(new Text(st.nextToken()), new Text(fileName));
            }
        }

    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            StringBuilder indexBuilder = new StringBuilder();
            HashSet<String> hashSet = new HashSet<String>();
            for (Text value :values) {
                String filename = value.toString();
                if (hashSet.contains(filename)) {
                    continue;
                }
                hashSet.add(filename);
                indexBuilder.append(filename).append("|");
            }
            context.write(key, new Text(indexBuilder.toString()));
        }

    }

    public static void main (String args[]) throws Exception{
        if (args.length < 2) {
            System.err.println("Input and output paths is needed");
            System.exit(0);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 1 : 0);
    }
}
