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
		//�������·����Ҫ�����Լ�������ʵ�ʵ��������·������
		 args=new String [] {"d:/input/inputflow","e:/output1"};
		 
		 //1.��ȡ������Ϣ����job����
		 Configuration configuration = new Configuration ();
		 Job job = Job.getInstance(configuration);
		 
			// 6 ָ���������jar�����ڵı���·��
			job.setJarByClass(FlowsumDriver.class);

		 
		 //2.ָ��ҵ��jobҪʹ�õ�Mapper/Reducecҵ������
		 job.setMapperClass(FlowCountMapper.class);
		 job.setReducerClass(FlowCountReducer.class);
		 
		 //3.ָ��mapper������ݵ�kv����
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(FlowBean.class);
		
		 
		 //4.ָ�������������������kv����
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(FlowBean.class);
		 
		 //5.ָ��job������ԭʼ�ļ����ڵ�Ŀ¼
		 FileInputFormat.setInputPaths(job,new Path(args[0]));
		 FileOutputFormat.setOutputPath(job,new Path(args[1]));
		 
		 //7.��job�����õ���ز������Լ�job���� ��java ������jar��������yarnȥ����
		 
		 boolean result = job.waitForCompletion(true);
		 System.out.println(result ? 0:1);
		 
	}
}
