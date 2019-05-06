package cn.gulu.bigdata.mr.fensi;

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
import java.lang.reflect.Array;
import java.util.Arrays;

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
* 1.第二阶段的map任务获取的数据是怎样的
* 2.数据的提取
* 3.找出明星之间共同粉丝的过程
* 4.第二阶段的map输出是怎样的
* */
public class SharedFriendsStepTwo {

    static class SharedFriendsStepTwoMapper extends Mapper<LongWritable, Text,Text,Text>{
        //1.map阶段获取的数据
        //A I,K,C,B,G,F,H,O,D
        //A是粉丝，I,K,C,B,G,F,H,O,D是明星
        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] friend_persons = line.split("\t");

            //2.提取出粉丝和明星列表
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");

            Arrays.sort(persons);

            //3.具体匹配共同粉丝的过程：在每个粉丝喜欢的明星列表遍历，两两相互比较
            //如果两个明星有共同粉丝，那么就记录下来
            //4.第二阶段的map输出是<明星i-明星j,粉丝1>
            for(int i = 0;i < persons.length-2;i++){
                for(int j = i+1;j < persons.length-1;j++){
                    context.write(new Text(persons[i]+"-"+persons[j]),new Text(friend));
                }
            }
        }
    }

    static class SharedFriendsStepTwoReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text peron_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();

            for(Text friend:friends){
                stringBuffer.append(friend).append(" ");
            }
            //5.第二阶段的reduce输出是<明星i-明星j,粉丝1,粉丝2,粉丝3,粉丝4...>
            context.write(peron_person,new Text(stringBuffer.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendsStepTwo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(SharedFriendsStepTwoMapper.class);
        job.setReducerClass(SharedFriendsStepTwoReducer.class);

        job.waitForCompletion(true);
    }
}
