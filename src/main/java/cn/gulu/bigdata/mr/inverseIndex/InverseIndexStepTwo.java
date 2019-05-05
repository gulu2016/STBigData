package cn.gulu.bigdata.mr.inverseIndex;

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
 * @Package: cn.gulu.bigdata.mr.inverseIndex
 * @ClassName: InverseIndexStepTwo
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-5 下午3:33
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-5 下午3:33
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 1.第二阶段的mapper任务是什么
* 2.第二阶段的reducer任务是什么
* */
public class InverseIndexStepTwo {

    public static class InverseIndexStepTwoMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        //1.第二阶段的mapper任务就是将<word-filename,n>
        //封装成<word,filename n>
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("-");
            context.write(new Text(line[0]),new Text(line[1]));
        }
    }

    public static class InverseIndexStepTwoReducer extends Reducer<Text,Text,Text,Text> {
        @Override

        //2.reducer的任务就是将<word,filename1 n1><word,filename2 n2>
        // 聚合成<word,filename->n filename2->n2>
        protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            for(Text text:values){
                stringBuffer.append(text.toString().replace(" ","->")+"\t");
            }
            context.write(key,new Text(stringBuffer.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setMapperClass(InverseIndexStepTwoMapper.class);
        job.setReducerClass(InverseIndexStepTwoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?1:0);
    }
}
