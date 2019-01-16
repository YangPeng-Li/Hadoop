package com.lyp.mr.worldcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * KEYIN, map�׶������key 
 * VALUEIN, map�׶������value
 * KEYOUT,
 * VALUEOUT
 * 
 * @author Lyp
 * @date 2019��1��16��
 * @version 1.0
 *
 */
public class WordCountReduce extends Reducer<Text ,IntWritable,Text,IntWritable>
{
	IntWritable  v = new IntWritable();
	int sum = 0;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
	{
		
		//1.�ۼ����
		for (IntWritable value : values) 
		{
			 
			 sum +=value.get();
		} 
		//2.д�� lyp2
		v.set(sum);
		
		context.write(key, v);
	}
	
}
