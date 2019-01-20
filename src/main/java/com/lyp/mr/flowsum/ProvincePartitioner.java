package com.lyp.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * �������ֻ��Ž��з������(��ͬʡ�ݷ��ڲ�ͬ�ķ�������)
 * 
 * key��Map�е��ֻ��� key value ��Map ������������
 * 
 * @author Lyp
 * @date 2019��1��20��
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
		// key ���ֻ���
		// value���ֻ�����
		// ��ȡ�ֻ���ǰ��λ

	}

}
