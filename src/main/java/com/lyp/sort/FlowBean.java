package com.lyp.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * 
 * @author Lyp
 * @date 2018��1��21��
 * @version 1.0
 *
 */

public class FlowBean implements WritableComparable<FlowBean> {

	private long upFlow;//��������
	private long downFlow;//��������
	private long sumFlow;//������

	// ���л�����
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	// �����л�ʱע�����˳��һ��
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	@Override
	public int compareTo(FlowBean bean) {

		// �����������Ĵ�С��������
		//���ıȽ������ж�(�������򣬰����������Ĵ�С����)
		int result;
		if (sumFlow > bean.getSumFlow()) {
			result = -1;
		} else if (sumFlow < bean.getSumFlow()) {
			result = 1;
		} else {
			result = 0;
		}

		return result;
	}

	// �����л�ʱ����Ҫ�ղι��캯��
	public FlowBean() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = downFlow + upFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

}
