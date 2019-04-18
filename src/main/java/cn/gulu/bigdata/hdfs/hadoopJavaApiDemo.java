package cn.gulu.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ProjectName: ChuanzhiBigdata
 * @Package: cn.zhangjiaqian.bigdata.hdfs
 * @ClassName: hadoopJavaApiDemo
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-18 下午1:52
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-18 下午1:52
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class hadoopJavaApiDemo {

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
        //file://是一个传输协议
        //比如可以使用file:///home/zhangjiaqian/hive/testData访问本地文件
        fs.copyFromLocalFile(new Path("file:///home/zhangjiaqian/hive/testData"),new Path("/"));
        fs.close();
    }

    @Test
    public void testDownLoad() throws IOException {
        fs.copyToLocalFile(new Path("/testData"),new Path("/home/zhangjiaqian/"));
        fs.close();
    }

    @Test
    //创建新的文件夹
    public void testMakeDir() throws Exception {
        boolean mkdirs = fs.mkdirs(new Path("/x/y/z"));
        System.out.println(mkdirs);
    }

    @Test
    public void testDelete() throws Exception{
        //第二个参数为true是递归删除
        boolean delete = fs.delete(new Path("/x"), true);
        System.out.println(delete);
    }
}
