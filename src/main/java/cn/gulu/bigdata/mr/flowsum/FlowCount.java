package cn.gulu.bigdata.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * @ClassName: FlowCount
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
* */
public class FlowCount {

    static class FlowcountMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

        //map阶段的业务逻辑，被maptask调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将传入的数据按空格分割成单词
            String[] fields = value.toString().split("\t");

            //取出手机号，上行流量，下行流量
            String phoneNumber = fields[1];
            long upFlow = Long.parseLong(fields[5]);
            long downFole = Long.parseLong(fields[6]);

            context.write(new Text(phoneNumber),new FlowBean(upFlow,downFole));
        }
    }

    static class FlowcountReducer extends Reducer<Text, FlowBean,Text,FlowBean> {

        //reduce函数会被reduceTask任务调用，每一组单词调用一次
        //每一组的意思就是[<hello,1>,<hello,1>,<hello,1>]...
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_dFlow = 0;

            for(FlowBean bean:values){
                sum_upFlow += bean.getUpFlow();
                sum_dFlow += bean.getdFlow();
            }

            FlowBean resultBean = new FlowBean(sum_upFlow,sum_dFlow);
            context.write(key,resultBean);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job =  Job.getInstance(configuration);

        //指定本程序jar包所在的路径
        job.setJarByClass(FlowCount.class);

        //利用反射指定job要使用的mapper业务类
        job.setMapperClass(FlowcountMapper.class);
        job.setReducerClass(FlowcountReducer.class);

        //利用反射，指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

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
