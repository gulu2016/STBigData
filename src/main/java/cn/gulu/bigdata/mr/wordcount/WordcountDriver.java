package cn.gulu.bigdata.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.mr.MRDemos
 * @ClassName: WordcountDriver
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-23 下午9:42
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-23 下午9:42
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

//该程序是配置mr运行条件，并启动mr的程序
//相当于一个yarn客户端，yarn是分配资源的
public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job =  Job.getInstance(configuration);

        //指定本程序jar包所在的路径
        job.setJarByClass(WordcountDriver.class);

        //利用反射指定job要使用的mapper业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //利用反射，指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置最终(也就是reduce)输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

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
