package com.lyp.mr.outputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FilterDriver {
	
	public static void main(String[] args) throws Exception {
		args = new String [] {"d:/io/outputformat","d:/output7"};
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FilterDriver.class);
		
		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//要将自定义的输出格式组件设置到job中
		job.setOutputFormatClass(FilterOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		
		System.exit(result ? 0:1);
	
	}

}
