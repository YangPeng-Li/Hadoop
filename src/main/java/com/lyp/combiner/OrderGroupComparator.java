package com.lyp.combiner;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * �˷������ã���bean������id��ͬ�ķŵ�һ�����У�Ŀ���ǽ���ͬһ���ȶԷ�����
 * 
 * @author Lyp
 * @date 2019��1��22��
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
	 * ����һ�����콫�Ƚ϶���ĵ��ഫ������
	 */
	protected OrderGroupComparator() {
		super(OrderBean.class, true);//�����Ϊfalse �򷵻ص�key��ֵ����Ϊnull
	}
	
	
	//�Ƚ�ҵ���߼�
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		 
		//ֻҪid��ͬ����Ϊ����ͬ��key
		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;
		int result;
		if (aBean.getOrder_id() > bBean.getOrder_id()) {
			result = 1;
		} else if (aBean.getOrder_id() < bBean.getOrder_id()) {
			result = -1;
		} else {
			result = 0;//��ID��ͬʱ���Ƚϼ۸�
		}
		return result;//ֻҪ����0������Ϊ��true,����reduce
	}
}
