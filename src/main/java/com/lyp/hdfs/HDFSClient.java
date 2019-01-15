package com.lyp.hdfs;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;
import org.junit.Test;



public class HDFSClient {
	
	private static  Logger logger = Logger.getLogger(HDFSClient.class);
	
	/**
	 *  上传文件
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void UpFile () throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration ();// 1.get FileSystem
		
		/*
		 * 参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的默认配置
		 */
		
		configuration.set("dfs.replication", "3");//replication
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lyp");
		
		
		fs.copyFromLocalFile(new Path("D:/banzhang.txt"), new Path("/banzhang.txt"));//2. up FileSystem
		fs.close();
		
		logger.info("HDFS FileSystem UP Over");
	}

	/**
	 * 下载
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void DownloadFile () throws IOException, InterruptedException, URISyntaxException
	{
		
		Configuration configuration = new Configuration();
		//configuration.set("hdfs.replication", "3");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),configuration ,"lyp");
		//Path dst 
		// boolean useRawLocalFileSystem 是否开启文件校验
		/**
		 * 
		 * fs.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
		 * @boolean delSrc 是否将源文件删除
		 * @Path src 指定要下载的文件
		 * @Path dst 将文件下载到路径
		 * @Boolean useRawLocalFileSystem 是否开启文件校验
		 * 
		 */
		fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("D:/banhua.txt"), true);
		fs.close();
		logger.info("HDFS FileSystem Download Over");
	}
	@Test
	public void DeleteFile () throws IOException, InterruptedException, URISyntaxException
	{
		Configuration configuration =new Configuration ();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lyp");
		
		fs.rename(new Path("/banzhang.txt"), new Path("tuanwei.txt"));//修改文件名字
		fs.delete(new Path("/1001/"),true);//执行删除
		
		fs.close();
	}
	
	/**
	 * 获取文件列表 listfiles
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void ListFile() throws IOException, InterruptedException, URISyntaxException
	{
		Configuration  configuration = new Configuration ();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lyp");
		//获取文件详情
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext())
		{
			LocatedFileStatus status = listFiles.next();
			//输出详情
			//文件名称
			logger.info(status.getPath().getName());
			//长度
			logger.info(status.getLen());
			//权限
			logger.info(status.getPermission());
			//分组
			logger.info(status.getGroup());
			
			//获取存储的块信息
			BlockLocation [] blockLocations = status.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations)
			{
				//获取块存储的主机节点
				String [] hosts = blockLocation.getHosts();
				for (String host : hosts)
				{
					System.out.println(host);
				}
			}
			System.out.println("------------------------分割线-------------------------");
		}
		fs.close();
	}
	
	
	/**
	 * 文件夹判断 liststatus
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void FileStatusInfo () throws IOException, InterruptedException, URISyntaxException
	{
			
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration , "lyp");
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus)
		{
			//如果是文件
			if (fileStatus.isFile())
			{
				logger.info("f："+fileStatus.getPath().getName());
			}
			else
			{
				logger.info("d："+fileStatus.getPath().getName());
			}
		}
		fs.close();
	}
}
