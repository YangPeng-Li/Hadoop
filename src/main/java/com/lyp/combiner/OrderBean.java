package com.lyp.combiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>  {

	private int order_id;// 订单ID
	private double order_price;// 价格

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(order_id);
		out.writeDouble(order_price);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		order_id = in.readInt();
		order_price = in.readDouble();
	}

	@Override
	public int compareTo(OrderBean bean) {
		// 按照id升序排序,如果价格相同
		int result;
		if (order_id > bean.getOrder_id()) {
			result = 1;
		} else if (order_id < bean.getOrder_id()) {
			result = -1;
		} else {
			if (order_price > bean.getOrder_price()) {
				result = -1;
			} else if (order_price < bean.getOrder_price()) {
				result = 1;
			} else {
				result = 0;
			}
		}
		return result;
	}

	public OrderBean() {
		super();
		// TODO Auto-generated constructor stub
	}

	public OrderBean(int order_id, double order_price) {
		super();
		this.order_id = order_id;
		this.order_price = order_price;
	}

	public int getOrder_id() {
		return order_id;
	}

	public void setOrder_id(int order_id) {
		this.order_id = order_id;
	}

	public double getOrder_price() {
		return order_price;
	}

	public void setOrder_price(double order_price) {
		this.order_price = order_price;
	}

	@Override
	public String toString() {
		return order_id + "\t" + order_price;
	}

}
