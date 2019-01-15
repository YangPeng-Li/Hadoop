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
		
		//1.��ȡ������Ϣ�Լ���װ����
		Configuration configuration = new  Configuration ();
		Job job = Job.getInstance(configuration);
		
		//2.����Jar����·��
		job.setJarByClass (WordCountDriver.class);
		
		//3.����Map ��Reduce ��
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		
		//4.����Map���
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//5.�����������KV����
		job.setOutputKeyClass (Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//6.������������·��
		FileInputFormat.setInputPaths(job,new Path(args [0]));
		FileInputFormat.setInputPaths(job,new Path(args [1]));
		
		//7���ύ
		boolean result = job.waitForCompletion(true);
		
		System.out.println(result ? 0:1);
		
		
	}

}
