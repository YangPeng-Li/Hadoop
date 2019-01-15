package com.lyp.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//1.获取配置信息以及封装任务
		Configuration configuration = new  Configuration ();
		Job job = Job.getInstance(configuration);
		
		//2.设置Jar加载路径
		job.setJarByClass (WordCountDriver.class);
		
		//3.设置Map 和Reduce 类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		
		//4.设置Map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//5.设置最终输出KV类型
		job.setOutputKeyClass (Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//6.设置输入和输出路径
		FileInputFormat.setInputPaths(job,new Path(args [0]));
		FileInputFormat.setInputPaths(job,new Path(args [1]));
		
		//7。提交
		boolean result = job.waitForCompletion(true);
		
		System.out.println(result ? 0:1);
		
		
	}

}
