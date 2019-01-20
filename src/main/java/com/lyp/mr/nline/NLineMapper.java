package com.lyp.mr.nline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * map阶段
 * LongWritable 输入数据的key （行的偏移量）
 * Text			
 * Text			输出数据key
 * IntWritable	
 * @author Lyp
 * @date 2018年1月18日
 * @version 1.0
 *
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text k = new Text();
	IntWritable V = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		//获取一行
		String line = value.toString();
		//切割
		String[] words = line.split(" ");
		//循环
		for (String str : words) 
		{
			k.set(str);
			context.write(k, V);
		}
		
		
	}
}
