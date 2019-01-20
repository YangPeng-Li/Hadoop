package com.lyp.mr.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * ��С���ļ����������Ƭ
 * K �ļ�·��+�ļ���
 * V  �ļ������ֽ���
 * 
 * @author Lyp
 * @date 2018��1��18��
 * @version 1.0
 *
 */

public class WholeFileReader extends RecordReader<Text,BytesWritable>{
	FileSplit fileSplit; 
	Configuration config;
	Text k= new Text();
	BytesWritable v = new BytesWritable();
	boolean isFlag = true;//ÿ��ִ��WholeFileReader ���ᴴ��
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		//��ʼ��
		fileSplit = (FileSplit) split;
		config=context.getConfiguration();
		
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		//����ҵ���߼�
		if (isFlag)
		{
			byte buf []= new byte[(int)fileSplit.getLength()] ;
			
			//��ȡFS(�ļ�����ϵͳ[FileSystem])
			Path path = fileSplit.getPath();
			FileSystem fs = path.getFileSystem(config);
			
			// ��ȡ������
			FSDataInputStream fis = fs.open(path);//�����ļ�ϵͳ�ҵ������ļ����ݣ�[����·����ȡ������]
			//����
			IOUtils.readFully(fis, buf, 0, buf.length);//���������뻺������
			
			//��װv
			v.set(buf, 0, buf.length);//�������������ݷ���V��
			k.set(path.toString());//�ļ�+�ļ�����
			
			//�ر���Դ
			IOUtils.closeStream(fis);
			
			isFlag=false;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return k;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return v;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		
	}

	

}
