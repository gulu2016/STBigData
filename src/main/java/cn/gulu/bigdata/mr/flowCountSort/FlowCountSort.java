package cn.gulu.bigdata.mr.flowCountSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.mr.flowsum
 * @ClassName: FlowCountSort
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-1 下午6:36
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-1 下午6:36
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*统计flowData文件中流量信息
* 文件中共有7列，第1列是手机号，第5列是上传流量，第6列是下载流量
* 任务就是统计每一个手机号对应的总共上行流量和下载流量，以及流量总和（上行流量+下载流量）
* -------------------------------------------------------------------------
* 以上是flowsum的任务，这里的附加任务是根据flow对输出进行排名（从高到低）
*
* 几个问题（答案在代码注释中）
* 1.对流量进行排序，流量信息应该作为map的key还是value
* 2.context.write(bean,phoneNum);多次使用同一个bean,phoneNum，结果是否会都相同
* 3.对流量的排序是如何做到的（对象作为key,对象类型可比较：重写了WritableComparable的compareTo方法）
* 4.输出信息格式如何确定
* */
public class FlowCountSort {

    static class FlowcountMapper extends Mapper<LongWritable, Text,FlowBean, Text> {

        //在此处定义变量可以避免每次调用map函数时候都创建对象
        FlowBean bean = new FlowBean();
        Text phoneNum = new Text();

        //map阶段的业务逻辑，被maptask调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将传入的数据按空格分割成单词
            String[] fields = value.toString().split("\t");

            //取出手机号，上行流量，下行流量
            String phonenum = fields[1];
            long upFlow = Long.parseLong(fields[5]);
            long downFlow = Long.parseLong(fields[6]);

            bean.set(upFlow,downFlow);
            phoneNum.set(phonenum);

            //1.这里使用bean做key的原因：map输出会根据key排序，而我们需要的就是根据流量sumFlow
            //排序，所以选择bean作为输出的key
            //2.对于每次调用map函数，虽然使用的bean和phoneNum对象是一样的，但是每次write时候
            //都需要序列化，所以每次写入的内容都不会相同
            //3.map函数会固定根据key对结果进行排序，此时使用的是bean作为key，那么bean必须是可比较的
            //这就需要在FlowBean中重写WritableComparable的compareTo方法
            context.write(bean,phoneNum);
        }
    }

    static class FlowcountReducer extends Reducer<FlowBean, Text,Text, FlowBean> {

        //reduce函数会被reduceTask任务调用，每一组单词调用一次
        @Override
        protected void reduce(FlowBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //4.输出时候按照<text,bean>格式进行，也就是手机号在前，流量信息在后
            context.write(values.iterator().next(),bean);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job =  Job.getInstance(configuration);

        //指定本程序jar包所在的路径
        job.setJarByClass(FlowCountSort.class);

        //利用反射指定job要使用的mapper业务类
        job.setMapperClass(FlowcountMapper.class);
        job.setReducerClass(FlowcountReducer.class);

        //利用反射，指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //设置最终(也就是reduce)输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job的输出结果
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //将job中配置的相关参数，提交给yarn运行
        //等待集群完成工作
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
