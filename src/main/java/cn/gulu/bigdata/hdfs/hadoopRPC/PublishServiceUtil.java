package cn.gulu.bigdata.hdfs.hadoopRPC;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;


/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.hdfs.hadoopRPC
 * @ClassName: PublishServiceUtil
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-20 下午4:28
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-20 下午4:28
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

//服务端的启动程序
public class PublishServiceUtil {
    public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
        Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("localhost")
                .setPort(8888)
                //设置RPC协议为自定义的ClientNamenodeProtocol接口
                .setProtocol(ClientNamenodeProtocol.class)
                .setInstance(new MyNameNode());
        Server server = builder.build();
        server.start();

    }
}


