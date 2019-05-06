package cn.gulu.bigdata.mr.secondarysort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
*问题：以Order_000000x作为分组依据，查找每组的订单金额最大值
*/

/**
 * 
 * 1.maptask的任务
 * 2.中间过程是怎样的（2-1,2-2）
 * 3.reducetask的任务
 */
public class SecondarySort {

	//1.maptask的任务：将输入封装成<OrderBean,订单价格>
	static class SecondarySortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
		
		OrderBean bean = new OrderBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = StringUtils.split(line, ",");
			
			bean.set(new Text(fields[0]), new DoubleWritable(Double.parseDouble(fields[2])));
			
			context.write(bean, NullWritable.get());
			
		}
		
	}


	//3.reduceTask的任务：到达reduce时，相同id的所有bean已经被看成一组，且金额最大的那个一排在第一位
	static class SecondarySortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{

		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SecondarySort.class);
		
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);
		
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//2-2虽然reducetask是拿到一组数据，但是要多次调用reduce函数的
        //这里的分组依据默认是依据key值不相同，这里指定新的GroupingComparatorClass
        //也就是依据OrderBean的itemid作为分组依据
		job.setGroupingComparatorClass(ItemidGroupingComparator.class);

        //2-1.中间过程，由于指定PartitionerClass，并设置ReduceTasks数量为2
        //那么所有的maptask输出将会被分为两个分组
		job.setPartitionerClass(ItemIdPartitioner.class);
		job.setNumReduceTasks(2);
		
		job.waitForCompletion(true);
		
	}

}
