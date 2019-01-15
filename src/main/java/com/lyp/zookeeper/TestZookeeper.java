package com.lyp.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

public class TestZookeeper {
	
	private static Logger logger ;
	private String connectString ="hadoop102:2181,hadoop103:2181,hadoop104:2181"; //vim zoo.conf  clientport 2181
	private int sessionTimeout = 2000 ; //2�볬ʱʱ��	 
//	private Watcher watcher ;
	private ZooKeeper zkClient;
	
	@Before
	public void init () throws IOException 
	{
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				//���м���
				List<String> children;
				try {
					logger.info("---------Start---------");
					children = zkClient.getChildren("/", true);
					//ʵ�ּ�����ҪΪtrue
					for (String child: children)
					{
						System.out.println(child);
					}
					logger.info("---------end---------");
					
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}
	
	
	/**
	 *  create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
	 *   throws KeeperException, InterruptedException
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * 
	 * 
	 * CreateMode.EPHEMERAL_SEQUENTIAL���ݵĴ���ŵ�
	 * CreateMode.PERSISTENT_SEQUENTIAL�־õĴ����ŵ�
	 * 
	 */
	//1.�����ڵ� 
	@Test
	public void createNode () throws KeeperException, InterruptedException 
	{
		//	CreateMode createMode = null;
		String path = zkClient.create("/ZH_JS_NJ_QH", "handsomelyp".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		System.out.println(path);
	}
	//2.��ȡ�ӽڵ㲢��ء��ڵ㡿���ݵı仯
	
	@Test
	public void getDataAndWatcher () throws KeeperException, InterruptedException
	{
		List<String> children = zkClient.getChildren("/", true);//ʵ�ּ�����ҪΪtrue
		for (String child: children)
		{
			System.out.println(child);
		}
		 Thread.sleep(Long.MAX_VALUE);
	}
	//3.�жϽڵ��Ƿ����
	
	@Test
	public void exited () throws KeeperException, InterruptedException
	{
		Stat exists = zkClient.exists("/ZH_JS_NJ_QH", false);
		logger.info("�жϽڵ��Ƿ����"+exists == null? "Not exist ":"exite");
	}
	
}
