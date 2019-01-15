package com.lyp.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * �ѱ���ĳ���̵��ı��ϴ���HDFS ����
 * 
 * 
 * @author Lyp
 * @date 2019��1��7��
 * @version 1.0
 *
 */
public class IOAndHDFS {
	
	/**
	 * �ļ��ϴ�
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void UpFileToHDFS () throws IOException, InterruptedException, URISyntaxException
	{
		//1.��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI(""), configuration , "lyp");
		// ����������
		FileInputStream fis = new FileInputStream (new File ("D:/SchoolFlower.txt"));
		// ��ȡ������
		FSDataOutputStream fos = fs.create(new Path("/SchoolFlower.txt"));
		// ���Կ�
		IOUtils.copyBytes(fis, fos, configuration);
		
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
	
	@Test
	public void DownloadFile () throws IOException, InterruptedException, URISyntaxException
	{
		Configuration configuration = new Configuration ();
		FileSystem fs = FileSystem.get(new URI(""), configuration, "lyp");
		FSDataInputStream fis = fs.open(new Path("/SchoolFlower.txt"));
		FileOutputStream  fos = new FileOutputStream (new File ("D:/SchoolFlower.txt"));
		IOUtils.copyBytes(fis, fos, configuration);
		
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
	
	/**
	 * ��λ�ļ���ȡ
	 * ��ͬ�Ŀ������[���ص�һ��]
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void downloadBlock () throws IOException, InterruptedException, URISyntaxException
	{
		//1.��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration ();
		FileSystem fs =FileSystem.get(new URI(""),configuration,"lyp");
		
		//2.��ȡ������
		FSDataInputStream fis = fs.open(new Path ("/2.7.2.tar.gz"));
		//3.���������
		FileOutputStream fos =	new FileOutputStream (new File ("D:/2.7.2.tar.gz.part1"));
		//4.���Ŀ���
		byte [] bufs = new byte [1024];
		for (int i =0 ;i<1024*128;i++)//��ʵ�����
		{
			fis.read(bufs);
			fos.write(bufs);
		}
		//5.�ر���Դ		
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}
	
	/**
	 * ��λ�ļ���ȡ
	 * ��ͬ�Ŀ������[���صڶ���]
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void readFileSeek2 () throws IOException, InterruptedException, URISyntaxException
	{
		//1.��ȡ�ļ�ϵͳ
		Configuration configuration  = new Configuration ();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lyp");
		//2.��������
		FSDataInputStream fis = fs.open(new Path ("/2.7.2.tar.gz"));
		//3.��λ��������λ��
		fis.seek(1024*1024*128);
		//4.���������
		FileOutputStream fos = new FileOutputStream (new File ("D:/2.7.2.tar.gz.part2"));
		//5.���Կ�
		IOUtils.copyBytes(fis, fos, configuration);
		//6.�ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}
	
	
	/**  3 .�ϲ��ļ�
	 * Window������н��뵽Ŀ¼D:\��Ȼ��ִ��������������ݽ��кϲ�
	 * type 2.7.2.tar.gz.part2 >>2.7.2.tar.gz.part1
	 * �ϲ���ɺ󣬽�2.7.2.tar.gz.part1��������Ϊ2.7.2.tar.gz����ѹ���ָ�tar���ǳ�������
	 */
}
