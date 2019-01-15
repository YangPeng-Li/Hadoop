package com.lyp.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class FlowsumDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//输入输出路径需要根据自己电脑上实际的输入输出路径设置
		 args=new String [] {"d:/input/inputflow","e:/output1"};
		 
		 //1.获取配置信息或者job对象
		 Configuration configuration = new Configuration ();
		 Job job = Job.getInstance(configuration);
		 
			// 6 指定本程序的jar包所在的本地路径
			job.setJarByClass(FlowsumDriver.class);

		 
		 //2.指定业务job要使用的Mapper/Reducec业务类型
		 job.setMapperClass(FlowCountMapper.class);
		 job.setReducerClass(FlowCountReducer.class);
		 
		 //3.指定mapper输出数据的kv类型
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(FlowBean.class);
		
		 
		 //4.指定最终输出的数据类型kv类型
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(FlowBean.class);
		 
		 //5.指定job的输入原始文件所在的目录
		 FileInputFormat.setInputPaths(job,new Path(args[0]));
		 FileOutputFormat.setOutputPath(job,new Path(args[1]));
		 
		 //7.将job中配置的相关参数，以及job所用 的java 类所在jar包，交给yarn去运行
		 
		 boolean result = job.waitForCompletion(true);
		 System.out.println(result ? 0:1);
		 
	}
}
