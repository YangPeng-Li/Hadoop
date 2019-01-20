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
		
		//1.��ȡjob����
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		 
		
		//7.����NLineInputFormat�����¼��
		NLineInputFormat.setNumLinesPerSplit(job, 3);
		
		//8.ʹ��NlieInputFormat�����¼����
		job.setInputFormatClass(NLineInputFormat.class);
		
		//1.��ȡJar
		job.setJarByClass(NLineDriver.class);
		
		//2.��ȡMap��Reducer
		job.setMapperClass(NLineMapper.class);
		job.setReducerClass(NLineReducer.class);
		
		//3.Map��KV
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//4.���յ�KV
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//5��ȡ���·��ֵ
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//6 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0:1);
		
	}
}