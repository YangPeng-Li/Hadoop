package com.lyp.mr.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class KVDriver {
	
	public static void main(String[] args) throws Exception {
		args = new String [] {"d:/io/kv","d:/output3"};
		
		Configuration conf = new Configuration();
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf);
		
		//1.获取Jar
		job.setJarByClass(KVDriver.class);
		
		//2.获取Map和Reducer
		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);
		
		//3.Map的KV
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//4.最终的KV
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		//5获取输出路径值
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//6 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0:1);
		
	}
}
