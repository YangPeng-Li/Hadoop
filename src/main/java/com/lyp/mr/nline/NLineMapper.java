package com.lyp.mr.nline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * map�׶�
 * LongWritable �������ݵ�key ���е�ƫ������
 * Text			
 * Text			�������key
 * IntWritable	
 * @author Lyp
 * @date 2018��1��18��
 * @version 1.0
 *
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text k = new Text();
	IntWritable V = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		//��ȡһ��
		String line = value.toString();
		//�и�
		String[] words = line.split(" ");
		//ѭ��
		for (String str : words) 
		{
			k.set(str);
			context.write(k, V);
		}
		
		
	}
}
