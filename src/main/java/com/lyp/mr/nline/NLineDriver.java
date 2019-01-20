package com.lyp.mr.nline;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NLineDriver {

	public static void main(String[] args) throws Exception {
		args = new String [] {"d:/io/nline","d:/output3"};
		
		//1.获取job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		 
		
		//7.设置NLineInputFormat处理记录数
		NLineInputFormat.setNumLinesPerSplit(job, 3);
		
		//8.使用NlieInputFormat处理记录条数
		job.setInputFormatClass(NLineInputFormat.class);
		
		//1.获取Jar
		job.setJarByClass(NLineDriver.class);
		
		//2.获取Map和Reducer
		job.setMapperClass(NLineMapper.class);
		job.setReducerClass(NLineReducer.class);
		
		//3.Map的KV
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//4.最终的KV
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//5获取输出路径值
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//6 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0:1);
		
	}
}