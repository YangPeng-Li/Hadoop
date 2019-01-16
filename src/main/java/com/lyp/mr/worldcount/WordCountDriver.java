package com.lyp.mr.worldcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDriver {
	public static void main(String[] args) throws IOException, ReflectiveOperationException, InterruptedException {

		Configuration conf = new Configuration();
		// 1.获取job对象
		Job job = Job.getInstance(conf);

		// 2.设置jar存储位置
		job.setJarByClass(WordCountDriver.class);

		// 3.关联Map和Reduce类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);

		// 4.设置Mapper阶段输出数据的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5.设置最终数据输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 6.设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //输出文件夹不能存在，存在会报错
		// 7.提交job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);//系统自己的打印
//		job.submit();

	}

}
