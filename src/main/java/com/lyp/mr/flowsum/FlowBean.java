package com.lyp.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 手机 上下行流量统计 
 * 号码为k 流量为v
 * 
 * 自定义bean 需要实现Writable接口
 * 
 * @author Lyp
 * @date 2018年1月17日
 * @version 1.0
 *
 */
public class FlowBean implements Writable{
	
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	//空参构造反射需要
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
	
	//序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
		
	}

	//反序列化方法
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
