package com.marlabs.bigdata.FriendRecommendation;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RecommendFriend {
	public static class RecommendMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			StringTokenizer token = new StringTokenizer(line);
			context.write(new Text(token.nextToken()), new Text(token.nextToken() + '\t' + token.nextToken()));
		}
	}
	
	public static class RecommendReducer extends Reducer<Text,Text,Text,Text>{
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			StringBuilder builder = new StringBuilder();
			Comparator<MyEntry<java.lang.String, java.lang.Integer>> comparator = new ValueComparator();
			PriorityQueue<MyEntry> queue = new PriorityQueue<MyEntry>(6,(Comparator<? super MyEntry>) comparator);
			for(Text value: values){
				StringTokenizer token = new StringTokenizer(value.toString());
				String friendId = token.nextToken();
				int commonNums = Integer.parseInt(token.nextToken());
				MyEntry<String,Integer> friend = new MyEntry<>(friendId,commonNums);
				queue.add(friend);
				if(queue.size()>5){
					removeMin(queue);
				}
			}
			String[] result = new String[queue.size()];
			int index = 0;
			while(index<result.length){
				result[index] = (java.lang.String) queue.poll().getKey();
				index++;
			}
			for(int i = result.length-1;i>=0;i--){
				builder.append(result[i] + "\t");
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
		job.setJarByClass(RecommendFriend.class);
		job.setJobName("Recommend friend");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(RecommendMapper.class);
		job.setReducerClass(RecommendReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static void removeMin(PriorityQueue<MyEntry> queue){
		MyEntry<String,Integer> minFriend = queue.peek();
		for(MyEntry<String,Integer> friend: queue){
			if(minFriend.getValue() > friend.getValue()){
				minFriend = friend;
			}
		}
		queue.remove(minFriend);
	}
}

final class MyEntry<String,Integer> implements Map.Entry<String,Integer>{

	private final String key;
	private Integer value; 
	
	public MyEntry(String key,Integer value){
		this.key = key;
		this.value = value;
	}
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return this.key;
	}

	@Override
	public Integer getValue() {
		// TODO Auto-generated method stub
		return this.value;
	}

	@Override
	public Integer setValue(Integer value) {
		// TODO Auto-generated method stub
		this.value = value;
		return value;
	}
}

class ValueComparator implements Comparator<MyEntry<String,Integer>>{

	@Override
	public int compare(MyEntry<java.lang.String, java.lang.Integer> o1,
			MyEntry<java.lang.String, java.lang.Integer> o2) {
		// TODO Auto-generated method stub
		return (int) o1.getValue() - (int) o2.getValue();
	}
	
}
