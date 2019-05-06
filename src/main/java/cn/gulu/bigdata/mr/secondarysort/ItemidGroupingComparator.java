package cn.gulu.bigdata.mr.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 1.如何指定reduceTask将发送过来的bean以它的orderid作为分组依据
 *
 */
public class ItemidGroupingComparator extends WritableComparator {

	//1-1.传入作为key的bean的class类型
    // 以及制定需要让框架做反射获取实例对象
	protected ItemidGroupingComparator() {
		super(OrderBean.class, true);
	}
	

	//1-2.重写compare函数,让进入同一个reducetask的<k,v>对进入不同的
    //迭代器中进行迭代，也就是分成不同的<k,v>对组，进而将一组bean看成相同的key
    //比较两个bean时，指定只比较bean中的orderid，这样就可以将相同orderid
    //的<OrderBean,value>分区同一个迭代器中
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean abean = (OrderBean) a;
		OrderBean bbean = (OrderBean) b;

		return abean.getItemid().compareTo(bbean.getItemid());
		
	}

}
