package cn.gulu.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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

    @Test
    //测试修改文件夹的名称
    public void testRenameDir() throws IOException {
        boolean change = fs.rename(new Path("/x"),new Path("/xx"));
        System.out.println(change);
    }

    @Test
    //测试查看目录信息功能
    public void testListFiles() throws IOException {
        //第二个参数为true表示递归调用
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);

        while (listFiles.hasNext()){
            //打印各种文件信息，和命令行中执行 hdfs dfs -lsr / 效果一样
            LocatedFileStatus status = listFiles.next();
            System.out.println(status.getPath().getName());
            System.out.println(status.getBlockSize());
            System.out.println(status.getPermission());
            System.out.println(status.getLen());

            //打印文件的每个分组位置，每128M一个分组
            BlockLocation[] blockLocations = status.getBlockLocations();
            for(BlockLocation bl:blockLocations){
                System.out.println("block-length:"+bl.getLength()+"--"+
                        "block-offset:"+bl.getOffset());
                String[] hosts = bl.getHosts();

                for(String host:hosts){
                    System.out.println(host);
                }
            }
            System.out.println();
        }
    }

    @Test
    //查看文件以及文件夹信息
    public void testListAll() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        String flag = "d-----------------";
        for(FileStatus fileStatus:listStatus){
            if(fileStatus.isFile())
                flag = "f----------------";
            else
                flag = "d----------------";
            System.out.println(flag+fileStatus.getPath().getName());
        }
    }
}
