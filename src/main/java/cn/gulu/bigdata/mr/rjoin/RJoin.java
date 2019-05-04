package cn.gulu.bigdata.mr.rjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.sound.sampled.Line;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.mr.rjoin
 * @ClassName: RJoin
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-4 下午4:20
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-4 下午4:20
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 1.关于Mapper泛型的设置
* 2.对数据的封装过程是怎样的
* 3.拼接两类数据形成最终结果
* */
public class RJoin {

    //1.关于Mapper泛型的设置：输入key是偏移量，使用LongWritable,输入value是字符串
    //输出key是连接的标识，因为要使同一个p_id的<k,v>对进入同一个ReduceTask进行连接
    //输出value是待连接的对象，InfoBean类型
    static class RJoinMapper extends Mapper<LongWritable, Text,Text,InfoBean>{
        InfoBean bean = new InfoBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();

            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String name = inputSplit.getPath().getName();
            String pid = "";

            //2.判断是order文件还是product文件，
            //根据文件不同内容填充bean
            if(name.startsWith("order")){
                String[] fields = line.split(",");
                pid = fields[2];
                bean.set(Integer.parseInt(fields[0]),fields[1],pid,Integer.parseInt(fields[3]),
                        "",0,0,"0");
            }else {
                String[] fields = line.split(",");
                pid = fields[0];
                bean.set(0,"",pid,0,fields[1],Integer.parseInt(fields[2]),Float.parseFloat(fields[3]),"1");
            }
            k.set(pid);
            context.write(k,bean);
        }
    }

    static class RJoinReducer extends Reducer<Text,InfoBean,InfoBean, NullWritable>{

        @Override
        protected void reduce(Text pid, Iterable<InfoBean> beans, Context context) throws IOException, InterruptedException {
            InfoBean pdBean = new InfoBean();
            InfoBean odBean = new InfoBean();
            ArrayList<InfoBean> orderBeans = new ArrayList<>();

            for (InfoBean bean:beans){
                if(bean.getFlag().equals("1")){
                    try {
                        BeanUtils.copyProperties(pdBean,bean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }else {
                    try {
                        BeanUtils.copyProperties(odBean,bean);
                        orderBeans.add(odBean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }

            //3.拼接两类数据形成最终结果
            for(InfoBean ansbean :orderBeans){
                ansbean.setPname(pdBean.getPname());
                ansbean.setCategory_id(pdBean.getCategory_id());
                ansbean.setPrice(pdBean.getPrice());

                context.write(ansbean,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job =  Job.getInstance(configuration);

        //指定本程序jar包所在的路径
        job.setJarByClass(RJoin.class);

        //利用反射指定job要使用的mapper业务类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        //利用反射，指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        //设置最终(也就是reduce)输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

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
