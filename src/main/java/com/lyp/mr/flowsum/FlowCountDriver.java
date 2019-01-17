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
 * @date 2018��1��17��
 * @version 1.0
 *
 *flowcount.txt�������ļ� ���ļ���ʽ��Ҫ���д����������д��󣬿Ӻܶࡣ����
 *"D:/io/inputflow",
 *"d:/output2
 *
 */
public class FlowCountDriver {
	
	public static void main(String[] args) throws IOException, Exception, InterruptedException {
		
		args = new String [] {"D:/io/inputflow","d:/output2"};
		
		// 1 ��ȡ������Ϣ�Լ���װ����job
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf);
		
		// 2 ����jar����·��
		job.setJarByClass(FlowCountDriver.class);
		
		// 3 ����map��reduce��
		job.setMapperClass(FlowCountMap.class);
		job.setReducerClass(FlowCountReducer.class);
		
		// 4 ����map���
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 5 �����������kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		// 6 ������������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7 �ύjob
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 :1);
	}
	}
