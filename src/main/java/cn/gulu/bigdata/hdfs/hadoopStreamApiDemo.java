package cn.gulu.bigdata.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.hdfs
 * @ClassName: hadoopStreamApiDemo
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-20 上午9:33
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-20 上午9:33
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

//使用流的方式操作hdfs
public class hadoopStreamApiDemo {
    FileSystem fs = null;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new  Configuration();
        //设置文件系统为hdfs
        //获取文件系统的客户端实例对象
        //注意：运行程序是要执行用户名为hadoop,否则会出现没写权限的情况
        //注意
        //9900是fileSystem的端口号
        //50070是namenode主节点的端口号
        //50090是namenode的secondarynamenode的端口号
        fs = FileSystem.get(new URI("hdfs://127.0.0.1:9900"),configuration,"hadoop");
    }

    @Test
    public void testUpload() throws IOException {
        FSDataOutputStream outputStream = fs.create(new Path("/xx/aa"),true);
        FileInputStream inputStream = new FileInputStream("/home/zhangjiaqian/hive/testData");

        //将本地文件拷贝到hdfs上
        IOUtils.copyBytes(inputStream,outputStream,4096);
    }

    @Test
    public void testDownLoadFileToLocal() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/xx/aa"));
        FileOutputStream outputStream = new FileOutputStream(new File("/home/zhangjiaqian/xx.new"));

        //将hdfs上的文件输入流传输到本地
        IOUtils.copyBytes(inputStream,outputStream,4096);
    }

    @Test
    public void testTandomAccess() throws IOException {
        FSDataInputStream in = fs.open(new Path("/xx/aa"));

        FileOutputStream outputStream = new FileOutputStream(new File("/home/zhangjiaqian/aa.new"));

        //从输入流中截取前19个字节
        IOUtils.copyBytes(in,outputStream,19L,true);
    }

    @Test
    //将hdfs上的文件内容打印到屏幕上(System.out)
    public void testCat() throws IOException {
        FSDataInputStream in = fs.open(new Path("/xx/aa"));
        IOUtils.copyBytes(in,System.out,1024);
    }
}
