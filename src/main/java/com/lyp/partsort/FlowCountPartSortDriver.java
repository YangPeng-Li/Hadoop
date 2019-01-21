package com.lyp.partsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ����ľ�������+����
 * 
 * @author Lyp
 * @date 2018��1��21��
 * @version 1.0
 *D:\output4
 */

public class FlowCountPartSortDriver {

	public static void main(String[] args) throws Exception {
		
		args = new String [] {"d:/io/sort","d:/output1"};

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowCountPartSortDriver.class);
		job.setMapperClass(FlowCountPartSortMapper.class);
		job.setReducerClass(FlowCountPartSortReducer.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		job.setPartitionerClass(ProvinceSortPartitioner.class);
		job.setNumReduceTasks(5);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		
		System.exit(result ? 0:1);
		
	}
}