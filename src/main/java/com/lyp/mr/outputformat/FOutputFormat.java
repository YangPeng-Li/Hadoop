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
			//1����ȡ�ļ�ϵͳ
			FileSystem fs = FileSystem.get(job.getConfiguration());
			//2.��������ļ�·��
			Path lypPath = new Path("d:/io/lyp.log");
			Path otherPath = new Path("d:/io/other.log");
			//3.���������
			 lypOut = fs.create(lypPath);
			 otherOut = fs.create(otherPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		//�ж��Ƿ����"lyp"�������ͬ�ļ�
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
			
		//�ر���Դ
		IOUtils.closeStream(lypOut);
		IOUtils.closeStream(otherOut);
	}
}
