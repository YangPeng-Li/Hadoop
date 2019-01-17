package com.lyp.mr.worldcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * KEYIN, map阶段输出的key 
 * VALUEIN, map阶段输出的value
 * KEYOUT,
 * VALUEOUT
 * 
 * @author Lyp
 * @date 2019年1月16日
 * @version 1.0
 *
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable v = new IntWritable();
	int sum;

	/**
	 * This method is called once for each key. Most applications will define their
	 * reduce class by overriding this method. The default implementation is an
	 * identity function.
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// 1.累加求和
		sum=0;
		for (IntWritable value : values) {
			
			sum += value.get();
		}
		
		// 2.写出 lyp2
		v.set(sum);
		System.out.println("----sum------"+sum);
		context.write(key, v);
	}
	
}
