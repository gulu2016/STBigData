package cn.gulu.bigdata.hdfs.hadoopRPC;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.hdfs.hadoopRPC
 * @ClassName: ClientNamenodeProtocol
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-20 下午4:25
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-20 下午4:25
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

//相当于调用RPC之间的协议
public interface ClientNamenodeProtocol {
    public static final long versionID = 1L;   // 会读取这个版本号,可以和客户端的不一样, 没有校验
    public String getMetaData(String path);
}
