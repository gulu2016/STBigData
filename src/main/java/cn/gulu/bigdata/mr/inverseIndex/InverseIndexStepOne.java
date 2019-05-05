package cn.gulu.bigdata.mr.inverseIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.mr.inverseIndex
 * @ClassName: InverseIndexStepOne
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-5 下午3:00
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-5 下午3:00
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 问题：将文件中的单词分文件做单词统计,比如针对a,b文件
* 统计过后的形式是wordA:fileA->3,fileB->3  wordB:fileA->2,fileB->2
* wordC:fileA->3,fileB->1
* */

/*
* 1.第一阶段的maptask任务是什么
* 2.第一阶段的reducetask的任务是什么
* */

public class InverseIndexStepOne {

    static class InverseIndexStepOneMapper extends Mapper<LongWritable, Text,Text, IntWritable>{

        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");

            //1.整个map的任务就是将每个单词封装成<word-filename,1>的格式（1-1,1-2）
            //1-1获取文件名
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            for(String word:words){
                //1-2将单词和文件名连接起来
                k.set(word+"-"+fileName);
                context.write(k,v);
            }
        }
    }

    static class  InverseIndexStepOneReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;

            //2.reduce阶段的任务：将<word-filename,1>聚合成<word-filename,n>
            for(IntWritable value:values){
                count += value.get();
            }

            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(InverseIndexStepOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(InverseIndexStepOneMapper.class);
        job.setReducerClass(InverseIndexStepOneReducer.class);

        job.waitForCompletion(true);
    }
}
