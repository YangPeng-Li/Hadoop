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
		// 1.��ȡjob����
		Job job = Job.getInstance(conf);

		// 2.����jar�洢λ��
		job.setJarByClass(WordCountDriver.class);

		// 3.����Map��Reduce��
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);

		// 4.����Mapper�׶�������ݵ�key��value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5.�����������������key��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 6.��������·�������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //����ļ��в��ܴ��ڣ����ڻᱨ��
		// 7.�ύjob
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);//ϵͳ�Լ��Ĵ�ӡ
//		job.submit();

	}

}
