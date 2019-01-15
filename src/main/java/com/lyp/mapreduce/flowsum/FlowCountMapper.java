package com.lyp.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	
	FlowBean v = new FlowBean();
	Text k = new  Text();
	
	protected void map (LongWritable key,Text value,Context context)
	{
		//1.获取一行
		String line = value.toString();
		//2.切割字段
		String[] fields = line.split("\t");
		//3.封装对象 取出手机号码
		String phoneNum = fields[1];
		//取出上下行流量
		long upFlow =Long.parseLong(fields[fields.length-3]);
		long downFlow =Long.parseLong(fields[fields.length-2]);
		k.set(phoneNum);
		
		//4.写出
		v.set(downFlow,upFlow);
		
		
	}
}
