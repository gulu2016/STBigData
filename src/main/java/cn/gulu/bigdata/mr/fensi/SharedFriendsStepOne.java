package cn.gulu.bigdata.mr.fensi;

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
 * @Package: cn.gulu.bigdata.mr.fensi
 * @ClassName: SharedFriendsStepOne
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-6 上午9:23
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-6 上午9:23
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 问题：找出两个人之间的共同好友，输入文件的每一行类似于A:B,C,D,F,E,O
* 代表着A的好友有B,C,D,F,E,O
* */

/*
* 1.文件原始内容是怎样的
* 2.第一阶段的map输出是怎样的
* 3.第一阶段的reduce输出是怎样的
* */
public class SharedFriendsStepOne {

    static class SharedFriendsStepOneMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            //1.文件一行的内容类似于A:B,C,D,F,E,O
            //可以理解为A是明星，B,C,D,F,E,O都是他的粉丝
            String line = value.toString();
            String[] person_friends = line.split(":");
            String person = person_friends[0];
            String friends = person_friends[1];

            for(String friend:friends.split(",")){
                //2.第一阶段的输出内容是<粉丝，明星>
                context.write(new Text(friend),new Text(person));
            }
        }
    }

    static class SharedFriendsStepOneReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();

            for(Text person:persons){
                stringBuffer.append(person).append(",");
            }
            //3.第二阶段的reduce输出是<粉丝，明星1,明星2,明星3...>
            context.write(friend,new Text(stringBuffer.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendsStepOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(SharedFriendsStepOneMapper.class);
        job.setReducerClass(SharedFriendsStepOneReducer.class);

        job.waitForCompletion(true);
    }
}
