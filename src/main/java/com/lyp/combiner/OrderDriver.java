package com.lyp.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * ������ˣ�ȡ�����������ĵ���
 * 
 * @author Lyp
 * @date 2018��1��21��
 * @version 1.0
 *
 */
public class OrderDriver {

	public static void main(String[] args) throws Exception {

		args = new String[] { "d:/io/order", "d:/ouput6" };

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(OrderDriver.class);
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// �÷��������ϵ
		job.setGroupingComparatorClass(OrderGroupComparator.class);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
