package com.lyp.mr.worldcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//map阶段
/**
 * KEYIN 输入数据的key 行的偏移量
 * VALUEIN 输入数据的value 
 * KEYOUT 输出数据的Key lyp,1 ss,1 
 * VALUEOUT输出数据的value
 * 
 * 
 * @author Lyp
 * @date 2018年1月16日
 * @version 1.0
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	IntWritable v = new IntWritable(1);
	Text k = new  Text ();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// lyp lyp
		//1.获取一行
		String line = value.toString();
		//2.切割单词
		String[] words = line.split(" ");
		//3.循环写出
		for (String word:words)
		{
			k.set(word);
			context.write(k, v);
		}
	
	}

}
