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
	 *  �ϴ��ļ�
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void UpFile () throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration ();// 1.get FileSystem
		
		/*
		 * �������ȼ����򣺣�1���ͻ��˴��������õ�ֵ >��2��ClassPath�µ��û��Զ��������ļ� >��3��Ȼ���Ƿ�������Ĭ������
		 */
		
		configuration.set("dfs.replication", "3");//replication
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lyp");
		
		
		fs.copyFromLocalFile(new Path("D:/banzhang.txt"), new Path("/banzhang.txt"));//2. up FileSystem
		fs.close();
		
		logger.info("HDFS FileSystem UP Over");
	}

	/**
	 * ����
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
		// boolean useRawLocalFileSystem �Ƿ����ļ�У��
		/**
		 * 
		 * fs.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
		 * @boolean delSrc �Ƿ�Դ�ļ�ɾ��
		 * @Path src ָ��Ҫ���ص��ļ�
		 * @Path dst ���ļ����ص�·��
		 * @Boolean useRawLocalFileSystem �Ƿ����ļ�У��
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
		
		fs.rename(new Path("/banzhang.txt"), new Path("tuanwei.txt"));//�޸��ļ�����
		fs.delete(new Path("/1001/"),true);//ִ��ɾ��
		
		fs.close();
	}
	
	/**
	 * ��ȡ�ļ��б� listfiles
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void ListFile() throws IOException, InterruptedException, URISyntaxException
	{
		Configuration  configuration = new Configuration ();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lyp");
		//��ȡ�ļ�����
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext())
		{
			LocatedFileStatus status = listFiles.next();
			//�������
			//�ļ�����
			logger.info(status.getPath().getName());
			//����
			logger.info(status.getLen());
			//Ȩ��
			logger.info(status.getPermission());
			//����
			logger.info(status.getGroup());
			
			//��ȡ�洢�Ŀ���Ϣ
			BlockLocation [] blockLocations = status.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations)
			{
				//��ȡ��洢�������ڵ�
				String [] hosts = blockLocation.getHosts();
				for (String host : hosts)
				{
					System.out.println(host);
				}
			}
			System.out.println("------------------------�ָ���-------------------------");
		}
		fs.close();
	}
	
	
	/**
	 * �ļ����ж� liststatus
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
			//������ļ�
			if (fileStatus.isFile())
			{
				logger.info("f��"+fileStatus.getPath().getName());
			}
			else
			{
				logger.info("d��"+fileStatus.getPath().getName());
			}
		}
		fs.close();
	}
}
