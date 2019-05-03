package cn.gulu.bigdata.mr.wordcountWithCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.mr.MRDemos
 * @ClassName: WordcountReducer
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-23 下午9:11
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-23 下午9:11
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

//KEYIN，VALUEIN对应mapper输出的KEYOUT，VALUEOUT
//KEYOUT，VALUEOUT是自定义的
//KEYOUT是单词，VALUEOUT是单词的总个数


public class WordcountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    //reduce函数会被reduceTask任务调用，每一组单词调用一次
    //每一组的意思就是[<hello,1>,<hello,1>,<hello,1>]...
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for(IntWritable value:values){
            //value表示从迭代器中取出的值，是统计的个数，要使用.get()转换函数转换成int类型
            count += value.get();
        }

        //key是单词，new IntWritable(count)是单词的总个数
        //其实context是将结果写入文件当中，文件存储在hdfs上
        context.write(key,new IntWritable(count));
    }
}
