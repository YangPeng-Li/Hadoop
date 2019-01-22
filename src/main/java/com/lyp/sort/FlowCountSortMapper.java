package com.lyp.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 现在流量是key,手机号是value
 * @author Lyp
 * @date 2018年1月21日
 * @version 1.0
 *
 */

public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
	FlowBean k = new FlowBean();
	
	Text v = new Text ();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
//		13736230513	2481	24681	27162
		//1、获取一行
		String line = value.toString();
		//2、切割
		String[] fields = line.split("\t");
		
		//3.封装对象
		String phoneNum = fields [0];
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		long sumFlow = Long.parseLong(fields[3]);
		
		k.setUpFlow(upFlow);
		k.setDownFlow(downFlow);
		k.setSumFlow(sumFlow);
		
		v.set(phoneNum);
			
		context.write(k, v);
	}
}
