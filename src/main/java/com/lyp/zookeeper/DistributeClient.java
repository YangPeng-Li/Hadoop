package com.lyp.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class DistributeClient {

	public static void main(String[] args) throws Exception {

		// 1.连接ZooKeeper
		DistributeClient client = new DistributeClient();

		client.getConnect();

		// 2.注册监听
		client.getChildren();

		// 3.业务逻辑处理
		client.business();
	}

	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	private int sessionTimeout = 2000;
	private ZooKeeper zkClient;

	private void getConnect() throws IOException {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				try {
					getChildren(); //动态监听
				} catch (IOException e) {
					e.printStackTrace();
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

	private void getChildren() throws IOException, KeeperException, InterruptedException {
		List<String> children = zkClient.getChildren("/servers", true);

		// 存储服务器节点主机名称
		ArrayList<String> hosts = new ArrayList<String>();

		for (String child : children) {
			byte[] data = zkClient.getData("servers/" + child, false, null);
			hosts.add(new String(data));
		}
		// 将所有在线主机名称打印到控制台
		System.out.println(hosts);
	}

	private void business() throws Exception {
		Thread.sleep(Long.MAX_VALUE);
	}

}
