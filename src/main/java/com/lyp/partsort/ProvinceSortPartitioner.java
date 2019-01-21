package com.lyp.partsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * �����ǣ���ͳ�ƽ�������ֻ��Ź����ز�ͬʡ���������ͬ�ļ��� �˹�����Ҫ��flowsum����ʹ��,ʹ�õ�������flowsum�����֮�������
 * 
 * @author Lyp
 * @date 2018��1��21��
 * @version 1.0
 *
 *          ���� ��K ��Map �����FlowBean :V ��Map �����Text
 *
 */
public class ProvinceSortPartitioner extends Partitioner<FlowBean, Text> {

	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		// 1.��ȡ�ֻ�ǰ��λ
		String prePhoneNum = value.toString().substring(0, 3);
		// 2.�ж����ĸ�ʡ
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
