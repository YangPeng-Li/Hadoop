package com.lyp.mr.worldcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDriver {
	public static void main(String[] args) throws IOException, ReflectiveOperationException, InterruptedException {
		args = new String [] {"D:/io/inputflow","d:/output2"};
				// 	1 获取配置信息以及封装任务
				Configuration configuration = new Configuration();
				Job job = Job.getInstance(configuration);

				// 2 设置jar加载路径
				job.setJarByClass(WordCountDriver.class);

				// 3 设置map和reduce类
				job.setMapperClass(WordCountMapper.class);
				job.setReducerClass(WordCountReduce.class);

				// 4 设置map输出
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);

				// 5 设置最终输出kv类型
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				//必须放在输出之后
				job.setInputFormatClass(CombineTextInputFormat.class);//改变默认的切片的模式
				CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);//切片大小设置为20M
				//2019-01-17 22:49:51,830 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1 	1片	
				
				// 6 设置输入和输出路径
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));

				// 7 提交
				boolean result = job.waitForCompletion(true);

				System.exit(result ? 0 : 1);
			}
		}
