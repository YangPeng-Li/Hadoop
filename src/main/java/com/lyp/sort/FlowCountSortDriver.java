package com.lyp.sort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 全排序
 * 
 * @author Lyp
 * @date 2018年1月21日
 * @version 1.0
 *
 */


public class FlowCountSortDriver {
	
	public static void main(String[] args) throws Exception {
		
		args = new  String [] {"d:/io/sort","d:/output4"};
		
		Configuration conf = new Configuration();
		Job job	= Job.getInstance(conf);
		
		job.setJarByClass(FlowCountSortDriver.class);
		
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReduce.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
/*		//关联分区
		job.setPartitionerClass(ProvincePartitioner.class);
		job.setNumReduceTasks(5);*/
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path (args[1]));
		
		boolean result = job.waitForCompletion(true);
		
		System.exit(result ? 0:1);
		
	}

}
