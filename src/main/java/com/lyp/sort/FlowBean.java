package com.lyp.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * 
 * @author Lyp
 * @date 2018年1月21日
 * @version 1.0
 *
 */

public class FlowBean implements WritableComparable<FlowBean> {

	private long upFlow;//上行流量
	private long downFlow;//下行流量
	private long sumFlow;//总流量

	// 序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	// 反序列化时注意参数顺序一致
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

		// 按照总流量的大小进行排序
		//核心比较条件判断(倒序排序，按照总流量的大小排序)
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

	// 反序列化时，需要空参构造函数
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
