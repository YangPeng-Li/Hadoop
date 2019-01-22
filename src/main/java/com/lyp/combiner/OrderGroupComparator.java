package com.lyp.combiner;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * 此方法作用：将bean对象中id相同的放到一个组中，目的是进入同一个比对方法中
 * 
 * @author Lyp
 * @date 2019年1月22日
 * @version 1.0
 *
 */
public class OrderGroupComparator extends WritableComparator {

	/**
	 * protected WritableComparator(Class<? extends WritableComparable> keyClass,
	 * boolean createInstances) 
	 * { 
	 * 		this(keyClass, null, createInstances); 
	 * }
	 * 
	 * protected WritableComparator(Class<? extends WritableComparable> keyClass,
	 * Configuration conf, boolean createInstances) 
	 * { 
	 * 		this.keyClass = keyClass;
	 * 		this.conf = (conf != null) ? conf : new Configuration(); 
	 * if (createInstances)
	 * 	{
	 * 	 	key1 = newKey(); key2 = newKey(); buffer = new DataInputBuffer();
	 *  }
	 *   else 
	 *  {
	 * 		key1 = key2 = null; buffer = null;
	 *  }
	 */
	/**
	 * 创造一个构造将比较对象的的类传给父类
	 */
	protected OrderGroupComparator() {
		super(OrderBean.class, true);//如果置为false 则返回的key的值设置为null
	}
	
	
	//比较业务逻辑
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		 
		//只要id相同就认为是相同的key
		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;
		int result;
		if (aBean.getOrder_id() > bBean.getOrder_id()) {
			result = 1;
		} else if (aBean.getOrder_id() < bBean.getOrder_id()) {
			result = -1;
		} else {
			result = 0;//当ID相同时，比较价格
		}
		return result;//只要返回0，就认为是true,传到reduce
	}
}
