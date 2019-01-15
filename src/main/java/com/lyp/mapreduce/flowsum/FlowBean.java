package com.lyp.mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * ��д����ͳ�Ƶ�bean����
 * 
 * @author Lyp
 * @date 2019��1��10��
 * @version 1.0
 *
 */
//ʵ��Writable �ӿ�
public class FlowBean implements Writable {

	private long upFlow;
	private long downFlow;
	private long sumFlow;
	
	//2.�����л�ʱ����Ҫ������ÿղι�����
	public FlowBean() {
		super();
	}
	
	
	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow+downFlow;
	}
	//д���л�����
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}


	//4�����л�����
	//5�����л�����˳������д���л�������˳�����һ��
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}
	//��дtoString�������������ӡ���ı�
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return upFlow+"\t"+downFlow+"\t"+sumFlow;
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


	public void set(long downFlow, long upFlow) {
		this.downFlow = downFlow;
		this.upFlow = upFlow;
		
	}
	
	

}
