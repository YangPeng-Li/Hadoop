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

		// 1.����ZooKeeper
		DistributeClient client = new DistributeClient();

		client.getConnect();

		// 2.ע�����
		client.getChildren();

		// 3.ҵ���߼�����
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
					getChildren(); //��̬����
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

		// �洢�������ڵ���������
		ArrayList<String> hosts = new ArrayList<String>();

		for (String child : children) {
			byte[] data = zkClient.getData("servers/" + child, false, null);
			hosts.add(new String(data));
		}
		// �����������������ƴ�ӡ������̨
		System.out.println(hosts);
	}

	private void business() throws Exception {
		Thread.sleep(Long.MAX_VALUE);
	}

}
