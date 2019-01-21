package com.lyp.partsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountPartSortMapper extends Mapper <LongWritable,Text,FlowBean,Text> {

	FlowBean k =  new FlowBean ();
	 Text v = new Text();
	
	
	@Override
	protected void map(LongWritable key, Text values, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = values.toString();
		
		String[] fields = line.split("\t");
		
		String phoneNum =  fields [0];
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
