 package com.lyp.combiner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
	
	OrderBean k = new OrderBean();
	
	@Override
	protected void map(LongWritable key, Text values,
			Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		//1.��ȡһ��
		String line = values.toString();
		
		//2.�и�
		String[] fields = line.split("\t");
		//3.��װ����
		k.setOrder_id(Integer.parseInt(fields[0]));
		k.setOrder_price(Double.parseDouble(fields[2]));
		
		context.write(k, NullWritable.get());
	}

}
