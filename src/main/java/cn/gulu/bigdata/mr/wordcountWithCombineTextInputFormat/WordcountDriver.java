package cn.gulu.bigdata.mr.wordcountWithCombineTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
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

/*
1.在没有指定InputFormat情况下，默认128MB一个切片，不足128MB的页作为一个切片，
一个切片对应一个MapTask，
这样会产生一个问题，如果有大量的小文件，那么就会有大量的MapTask。
为了解决这个问题，可以指定InputFormat为CombineTextInputFormat，
它可以合并多个小文件到一个切片。
* */
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

        //设置InputFormat,默认使用TextInputFormat，小文件也会产生一个切片
        //指定InputFormat为combineTextInputFormat之后，多个小文件会划分为一个切片
        job.setInputFormatClass(CombineTextInputFormat.class);
        //设置切片的最大和最小尺寸
        //大于4194304字节的会被切分为两个切片
        //小于2097152字节的页会被切分为一个切片
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        CombineTextInputFormat.setMinInputSplitSize(job,2097152);

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
