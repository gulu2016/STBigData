package cn.gulu.bigdata.hdfs.hadoopRPC;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.hdfs.hadoopRPC
 * @ClassName: MyNamenode
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-20 下午4:26
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-20 下午4:26
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

public class MyNameNode implements ClientNamenodeProtocol {
    // 模拟namenode的业务方法之一:查询元数据
    @Override
    public String getMetaData(String path) {
        return path + " 3- {BLK_1,BLK_2} ... ";
    }
}
