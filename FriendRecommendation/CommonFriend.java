package com.marlabs.bigdata.FriendRecommendation;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class CommonFriend {
	public static class CommonFriendMapper extends Mapper <LongWritable,Text,Text,Text>{
		private static final Text minus= new Text("-1");
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] result = line.split("\\t");
			String userId = result[0];
			if(result.length!=1){ 
				String[] friendList = result[1].split(",");
					for(String friendId: friendList){
						context.write(new Text(userId + '\t' + friendId), minus);
					}
					if(friendList.length != 1){
						for(int i = 0;i<friendList.length-1;i++){
							for(int j = i+1;j<friendList.length;j++){
								context.write(new Text(friendList[i] + '\t' + friendList[j]), new Text(userId));
							}
						}
					}
			}
		}
	}
	
	public static class CommonFriendReducer extends Reducer <Text,Text,Text,IntWritable>{
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(Text value:values){
				if(value.toString().equals("-1")){
					sum = -1;break;
				}
				else{
					sum++;
				}
			}
			
			if (sum!=-1) {
				context.write(key, new IntWritable(sum));
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length!=2){
			System.err.println("Path error");
			System.exit(-1);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(CommonFriend.class);
		job.setJobName("common friend");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(CommonFriendMapper.class);
		job.setReducerClass(CommonFriendReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
