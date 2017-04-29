package com.marlabs.bigdata.InvertedIndex;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertedIndex {
	
	public static class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text>{ 
		
		@Override
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			String cleaned_line = value.toString().replaceAll("[^a-zA-Z\\s]", "");
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			StringTokenizer token = new StringTokenizer(cleaned_line);
			while(token.hasMoreTokens()){
				String word = token.nextToken();	
				context.write(new Text(word), new Text(fileName));
			}
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>{
		
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuilder builder = new StringBuilder();
			HashSet<String> path = new HashSet<>();
			for(Text value:values){
				String file = value.toString();
				if(!path.contains(file)){
					builder.append(file + "  |  ");
					path.add(file);
				}
			}
			context.write(key, new Text(builder.toString()));
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length != 2){
			System.err.println("path error");
			System.exit(-1);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(InvertedIndex.class);
		job.setJobName("InvertedIndex");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
