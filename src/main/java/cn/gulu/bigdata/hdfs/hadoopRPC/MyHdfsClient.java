package cn.gulu.bigdata.hdfs.hadoopRPC;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.hdfs.hadoopRPC
 * @ClassName: MyHdfsClient
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-20 下午4:29
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-20 下午4:29
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

public class MyHdfsClient {
    public static void main(String[] args) throws IOException {
        ClientNamenodeProtocol namenodeProxy = RPC.getProxy(ClientNamenodeProtocol.class,
                1L,
                new InetSocketAddress("localhost",8888),
                new Configuration());
        System.out.println("==================");
        System.out.println(namenodeProxy.getClass());
        System.out.println("==================");
        String metaData = namenodeProxy.getMetaData("/llp.txt");
        System.out.println("调用RPC返回的数据 : " + metaData);

    }
}

