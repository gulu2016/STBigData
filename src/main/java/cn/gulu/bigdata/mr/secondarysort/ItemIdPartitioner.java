package cn.gulu.bigdata.mr.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
* 1.为什么要重写Partitioner类的getPartition函数？
* */
public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean bean, NullWritable value, int numReduceTasks) {
		//1.为了让相同id的订单bean，会发往相同的partition，
        //所以重写getPartition函数
		return (bean.getItemid().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		
	}

}
