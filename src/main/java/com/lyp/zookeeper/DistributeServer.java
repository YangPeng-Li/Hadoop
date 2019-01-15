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
 * 监听服务节点上下线
 * 某分布式系统中，主节点可以有多台，可以动态上下线，任意一台客户端都能实现实时感知到主节点服务上下线
 */
/**
 * 
 * 
 * @author Lyp
 * @date 2019年1月15日
 * @version 1.0
 * Run As Configuration  Argsments hadoop102
 *
 */
public class DistributeServer {
	
	private static Logger logger;
	
	public static void main(String[] args) throws IOException, Exception, InterruptedException 
	{
		//1.连接Zookeeper集群
		
		DistributeServer server	=	new  DistributeServer();
		server.getConnection();
		
		//2.注册节点
		server.register(args[0]);
		
		//业务逻辑	
		server.busniess();
		
	}

	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";//不能出现空格
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
