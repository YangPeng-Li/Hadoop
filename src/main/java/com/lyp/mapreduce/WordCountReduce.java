package com.lyp.mapreduce;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	int sum;
	IntWritable v =new IntWritable ();
	protected void reduce (Text key,Iterable <IntWritable> values,Context context) throws IOException, InterruptedException
	{
		sum = 0;
		for (IntWritable count : values)
		{
			sum += count.get();
		}
		v.set(sum);
		context.write(key,v);
		
	}


}
