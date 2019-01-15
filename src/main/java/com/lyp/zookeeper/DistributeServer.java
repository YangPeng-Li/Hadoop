package com.lyp.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;

/*
 * ��������ڵ�������
 * ĳ�ֲ�ʽϵͳ�У����ڵ�����ж�̨�����Զ�̬�����ߣ�����һ̨�ͻ��˶���ʵ��ʵʱ��֪�����ڵ����������
 */
/**
 * 
 * 
 * @author Lyp
 * @date 2019��1��15��
 * @version 1.0
 * Run As Configuration  Argsments hadoop102
 *
 */
public class DistributeServer {
	
	private static Logger logger;
	
	public static void main(String[] args) throws IOException, Exception, InterruptedException 
	{
		//1.����Zookeeper��Ⱥ
		
		DistributeServer server	=	new  DistributeServer();
		server.getConnection();
		
		//2.ע��ڵ�
		server.register(args[0]);
		
		//ҵ���߼�	
		server.busniess();
		
	}

	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";//���ܳ��ֿո�
	private int sessionTimeout=2000;
	private ZooKeeper zkClient;
	
	
	private void getConnection() throws Exception   {
		 zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				
			}});
	}
	
	private void register(String hostName) throws IOException, KeeperException, InterruptedException {
		
		zkClient.create("/servers/server", hostName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		logger.info("%s is online ",hostName);
	}
	
	private void busniess() throws Exception {
		Thread.sleep(Long.MAX_VALUE);
	}

}
