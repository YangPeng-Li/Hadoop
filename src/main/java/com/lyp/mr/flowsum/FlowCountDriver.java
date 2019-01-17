package com.lyp.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * 
 * @author Lyp
 * @date 2018年1月17日
 * @version 1.0
 *
 *flowcount.txt是输入文件 对文件格式有要求有错误则运行有错误，坑很多。。。
 *"D:/io/inputflow",
 *"d:/output2
 *
 */
public class FlowCountDriver {
	
	public static void main(String[] args) throws IOException, Exception, InterruptedException {
		
		args = new String [] {"D:/io/inputflow","d:/output2"};
		
		// 1 获取配置信息以及封装任务job
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf);
		
		// 2 设置jar加载路径
		job.setJarByClass(FlowCountDriver.class);
		
		// 3 关联map和reduce类
		job.setMapperClass(FlowCountMap.class);
		job.setReducerClass(FlowCountReducer.class);
		
		// 4 设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 5 设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7 提交job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 :1);
	}
	}
