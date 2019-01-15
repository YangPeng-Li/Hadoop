package com.lyp.mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 编写流量统计的bean对象
 * 
 * @author Lyp
 * @date 2019年1月10日
 * @version 1.0
 *
 */
//实现Writable 接口
public class FlowBean implements Writable {

	private long upFlow;
	private long downFlow;
	private long sumFlow;
	
	//2.反序列化时，需要反射调用空参构造器
	public FlowBean() {
		super();
	}
	
	
	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow+downFlow;
	}
	//写序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}


	//4反序列化方法
	//5反序列话方法顺序必须和写序列化方法的顺序必须一致
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}
	//编写toString方法方便后续打印到文本
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
