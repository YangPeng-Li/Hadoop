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
				// 	1 ��ȡ������Ϣ�Լ���װ����
				Configuration configuration = new Configuration();
				Job job = Job.getInstance(configuration);

				// 2 ����jar����·��
				job.setJarByClass(WordCountDriver.class);

				// 3 ����map��reduce��
				job.setMapperClass(WordCountMapper.class);
				job.setReducerClass(WordCountReduce.class);

				// 4 ����map���
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);

				// 5 �����������kv����
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				//����������֮��
				job.setInputFormatClass(CombineTextInputFormat.class);//�ı�Ĭ�ϵ���Ƭ��ģʽ
				CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);//��Ƭ��С����Ϊ20M
				//2019-01-17 22:49:51,830 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1 	1Ƭ	
				
				// 6 ������������·��
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));

				// 7 �ύ
				boolean result = job.waitForCompletion(true);

				System.exit(result ? 0 : 1);
			}
		}
