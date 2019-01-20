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
 * 将小的文件整体进行切片
 * K 文件路径+文件名
 * V  文件内容字节流
 * 
 * @author Lyp
 * @date 2018年1月18日
 * @version 1.0
 *
 */

public class WholeFileReader extends RecordReader<Text,BytesWritable>{
	FileSplit fileSplit; 
	Configuration config;
	Text k= new Text();
	BytesWritable v = new BytesWritable();
	boolean isFlag = true;//每次执行WholeFileReader 都会创建
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		//初始化
		fileSplit = (FileSplit) split;
		config=context.getConfiguration();
		
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		//核心业务逻辑
		if (isFlag)
		{
			byte buf []= new byte[(int)fileSplit.getLength()] ;
			
			//获取FS(文件管理系统[FileSystem])
			Path path = fileSplit.getPath();
			FileSystem fs = path.getFileSystem(config);
			
			// 获取输入流
			FSDataInputStream fis = fs.open(path);//根据文件系统找到输入文件内容，[根据路径获取输入流]
			//拷贝
			IOUtils.readFully(fis, buf, 0, buf.length);//输入流放入缓冲区中
			
			//封装v
			v.set(buf, 0, buf.length);//将缓冲区的内容放入V中
			k.set(path.toString());//文件+文件名称
			
			//关闭资源
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
