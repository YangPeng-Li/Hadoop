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
 * 把本地某个盘的文本上传到HDFS 上面
 * 
 * 
 * @author Lyp
 * @date 2019年1月7日
 * @version 1.0
 *
 */
public class IOAndHDFS {
	
	/**
	 * 文件上传
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void UpFileToHDFS () throws IOException, InterruptedException, URISyntaxException
	{
		//1.获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI(""), configuration , "lyp");
		// 创建输入流
		FileInputStream fis = new FileInputStream (new File ("D:/SchoolFlower.txt"));
		// 获取输入流
		FSDataOutputStream fos = fs.create(new Path("/SchoolFlower.txt"));
		// 流对拷
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
	 * 定位文件读取
	 * 不同的块的下载[下载第一块]
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void downloadBlock () throws IOException, InterruptedException, URISyntaxException
	{
		//1.获取文件系统
		Configuration configuration = new Configuration ();
		FileSystem fs =FileSystem.get(new URI(""),configuration,"lyp");
		
		//2.获取输入流
		FSDataInputStream fis = fs.open(new Path ("/2.7.2.tar.gz"));
		//3.创建输出流
		FileOutputStream fos =	new FileOutputStream (new File ("D:/2.7.2.tar.gz.part1"));
		//4.流的拷贝
		byte [] bufs = new byte [1024];
		for (int i =0 ;i<1024*128;i++)//依实际情况
		{
			fis.read(bufs);
			fos.write(bufs);
		}
		//5.关闭资源		
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}
	
	/**
	 * 定位文件读取
	 * 不同的块的下载[下载第二块]
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void readFileSeek2 () throws IOException, InterruptedException, URISyntaxException
	{
		//1.获取文件系统
		Configuration configuration  = new Configuration ();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lyp");
		//2.打开输入流
		FSDataInputStream fis = fs.open(new Path ("/2.7.2.tar.gz"));
		//3.定位输入数据位置
		fis.seek(1024*1024*128);
		//4.创建输出流
		FileOutputStream fos = new FileOutputStream (new File ("D:/2.7.2.tar.gz.part2"));
		//5.流对拷
		IOUtils.copyBytes(fis, fos, configuration);
		//6.关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}
	
	
	/**  3 .合并文件
	 * Window命令窗口中进入到目录D:\，然后执行如下命令，对数据进行合并
	 * type 2.7.2.tar.gz.part2 >>2.7.2.tar.gz.part1
	 * 合并完成后，将2.7.2.tar.gz.part1重新命名为2.7.2.tar.gz。解压发现该tar包非常完整。
	 */
}
