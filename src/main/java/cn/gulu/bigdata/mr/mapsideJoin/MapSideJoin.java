package cn.gulu.bigdata.mr.mapsideJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.mr.mapsideJoin
 * @ClassName: MapSideJoin
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-5 上午10:28
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-5 上午10:28
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */


/*
* 1.在map端进行join需要什么，为什么这么做可以避免数据倾斜
* 2.为什么要缓存产品表，而不是订单表
* 3.setup方法的作用
* 4.pdInfoMap的作用
* 5.map函数拥有的数据是哪些
* 6.map函数的处理逻辑
* 7.map端join如何设置reducetask数量
* */
public class MapSideJoin {

    static class MapSideJoinMapper extends Mapper<LongWritable, Text,Text,NullWritable>{
        //4.使用map存放产品信息
        Map<String,String> pdInfoMap = new HashMap<>();

        Text k = new Text();
        /*
         * 3.通过阅读Mapper类的源码，可以发现setup方法是在map方法之前调用一次，
         * 所以可以使用setup方法对任务进行初始化
         * */
        @Override
        protected void setup(Context context) throws IOException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("product")));
            String line;

            while (StringUtils.isNotEmpty(line = br.readLine())){
                String[] fields = line.split(",");
                pdInfoMap.put(fields[0],fields[1]);
            }
            br.close();
        }

        //5.在map阶段拥有从inputsplit中的数据以及setup中建立的pdInfoMap缓存
        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            //6.处理逻辑：先从订单表中获取各个字段，提取出商品的pid并在商品表中查找对应的名称
            //将名称和订单记录写入到文件，完成连接
            String orderLine = value.toString();
            String[] fields = orderLine.split(",");
            String pdName = pdInfoMap.get(fields[2]);
            k.set(orderLine+","+pdName);
            context.write(k,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //1.缓存普通文件到task运行节点的工作目录,这样就可以在map端进行join操作
        //在map端进行join操作可以避免数据倾斜问题，因为没有reducetask,
        //就不会出现大量数据发往同一个reducetask的情况
        //2.缓存产品表而不是订单，是因为在maptask中，有多个订单记录，每个
        //订单记录需要和固定的产品表进行连接，
        //而且从空间上比较，产品表比较小，订单表比较大
        job.addCacheFile(new URI("file:///home/zhangjiaqian/IdeaProjects/STBigData/src/main/java/cn/gulu/bigdata/mr/mapsideJoin/product"));

        //7.map端join不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
