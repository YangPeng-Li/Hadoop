package com.lyp.mr.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FOutputFormat extends RecordWriter<Text, NullWritable> {

	FSDataOutputStream lypOut = null;
	FSDataOutputStream otherOut = null;
	public FOutputFormat(TaskAttemptContext job) {
		try {
			//1、获取文件系统
			FileSystem fs = FileSystem.get(job.getConfiguration());
			//2.创建输出文件路径
			Path lypPath = new Path("d:/io/lyp.log");
			Path otherPath = new Path("d:/io/other.log");
			//3.创建输出流
			 lypOut = fs.create(lypPath);
			 otherOut = fs.create(otherPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		//判断是否包含"lyp"输出到不同文件
		if(key.toString().contains("lyp"))
		{
			lypOut.write(key.toString().getBytes());
		}else
		{
			otherOut.write(key.toString().getBytes());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
		//关闭资源
		IOUtils.closeStream(lypOut);
		IOUtils.closeStream(otherOut);
	}
}
