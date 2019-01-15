package com.lyp.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	
	FlowBean v = new FlowBean();
	Text k = new  Text();
	
	protected void map (LongWritable key,Text value,Context context)
	{
		//1.��ȡһ��
		String line = value.toString();
		//2.�и��ֶ�
		String[] fields = line.split("\t");
		//3.��װ���� ȡ���ֻ�����
		String phoneNum = fields[1];
		//ȡ������������
		long upFlow =Long.parseLong(fields[fields.length-3]);
		long downFlow =Long.parseLong(fields[fields.length-2]);
		k.set(phoneNum);
		
		//4.д��
		v.set(downFlow,upFlow);
		
		
	}
}
