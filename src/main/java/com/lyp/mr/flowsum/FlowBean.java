package com.lyp.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * �ֻ� ����������ͳ�� 
 * ����Ϊk ����Ϊv
 * 
 * �Զ���bean ��Ҫʵ��Writable�ӿ�
 * 
 * @author Lyp
 * @date 2018��1��17��
 * @version 1.0
 *
 */
public class FlowBean implements Writable{
	
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	//�ղι��췴����Ҫ
		public FlowBean() {
			super();
			// TODO Auto-generated constructor stub
		}

		public FlowBean(Long upFlow, Long downFlow) {
			super();
			this.upFlow = upFlow;
			this.downFlow = downFlow;
			sumFlow = downFlow+upFlow;
		}
	
	//���л�����
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
		
	}

	//�����л�����
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
		
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow ;
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

	public void set(long upFlow2, long downFlow2) {
		upFlow	= upFlow2;
		downFlow=downFlow2;
		sumFlow = upFlow2+downFlow2;
	}
	
	
}
