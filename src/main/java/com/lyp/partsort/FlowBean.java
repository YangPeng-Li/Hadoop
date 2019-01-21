package com.lyp.partsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * ʵ����������
 * 
 * @author Lyp
 * @date 2018��1��21��
 * @version 1.0
 *
 */

public class FlowBean implements WritableComparable<FlowBean>{

	private long upFlow;
	private long downFlow;
	private long sumFlow;
	
	
	//�޲ι���ʵ�ַ���
	public FlowBean() {
		super();
	}
	

	public FlowBean(long upFlow, long downFlow, long sumFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = downFlow+upFlow;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow=in.readLong();
		downFlow=in.readLong();
		sumFlow=in.readLong();
	}

	@Override
	public int compareTo(FlowBean bean) {
		return  sumFlow > bean.getSumFlow() ? -1:1;
	}


	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
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
