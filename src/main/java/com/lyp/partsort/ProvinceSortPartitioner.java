package com.lyp.partsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 需求是：将统计结果按照手机号归属地不同省份输出到不同文件中 此过程需要和flowsum相结合使用,使用的数据是flowsum计算过之后的数据
 * 
 * @author Lyp
 * @date 2018年1月21日
 * @version 1.0
 *
 *          分区 ：K 是Map 输出的FlowBean :V 是Map 输出的Text
 *
 */
public class ProvinceSortPartitioner extends Partitioner<FlowBean, Text> {

	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		// 1.获取手机前三位
		String prePhoneNum = value.toString().substring(0, 3);
		// 2.判断是哪个省
		int partitions = 4;

		if ("136".equals(prePhoneNum)) {
			 partitions = 0;
		} else if ("137".equals(prePhoneNum)) {
			 partitions = 1;
		} else if ("138".equals(prePhoneNum)) {
			 partitions = 2;
		} else if ("139".equals(prePhoneNum)) {
			 partitions = 3;
		}

		return partitions;
	}

}
