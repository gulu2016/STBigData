package cn.gulu.bigdata.mr.wordcountWithCombiner;

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

/*
1.该程序和wordcount的不同就是使用了combiner，
combiner程序可以在mapper程序输出结果的时候对每个reduce分区进行汇总
由于在wordcount中combiner做的事情和reducer做的事情是一样的，所以这里直接使用WordcountReducer
类作为combiner
2.combiner使用需要注意业务场景，如果reduce但是对每个分区求平均数，那么就不适合中间使用combiner
这是因为局部汇总结果会导致最终结果不一致。
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

        //指定需要使用的combiner,在wordcount案例中，combiner就是局部汇总，
        //所以可以直接使用WordcountReducer类作为combiner
        job.setCombinerClass(WordcountReducer.class);

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
