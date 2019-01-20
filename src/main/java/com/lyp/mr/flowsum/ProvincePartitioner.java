package com.lyp.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 需求按照手机号进行分区存放(不同省份放在不同的分区里面)
 * 
 * key是Map中的手机号 key value 是Map 的上下总流量
 * 
 * @author Lyp
 * @date 2019年1月20日
 * @version 1.0
 *
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {

		String prePhoneNum = key.toString().substring(0, 3);

		int partition = 4;

		if ("136".equals(prePhoneNum)) {
			partition = 0;
		} else if ("137".equals(prePhoneNum)) {
			partition = 1;
		} else if ("138".equals(prePhoneNum)) {
			partition = 2;
		} else if ("139".equals(prePhoneNum)) {
			partition = 3;
		}

		return partition;
		// key 是手机号
		// value是手机流量
		// 获取手机号前三位

	}

}
