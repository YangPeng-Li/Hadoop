package com.lyp.mr.worldcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//map�׶�
/**
 * KEYIN �������ݵ�key �е�ƫ����
 * VALUEIN �������ݵ�value 
 * KEYOUT ������ݵ�Key lyp,1 ss,1 
 * VALUEOUT������ݵ�value
 * 
 * 
 * @author Lyp
 * @date 2018��1��16��
 * @version 1.0
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	IntWritable v = new IntWritable(1);
	Text k = new  Text ();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// lyp lyp
		//1.��ȡһ��
		String line = value.toString();
		//2.�и��
		String[] words = line.split(" ");
		//3.ѭ��д��
		for (String word:words)
		{
			k.set(word);
			context.write(k, v);
		}
	
	}

}
