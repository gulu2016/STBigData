package cn.gulu.bigdata.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.hbase
 * @ClassName: HBaseTest
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-20 上午9:45
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-20 上午9:45
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 1.@Before要做的事情(1.1-1.4)
* 2.如何创建表
* 3.删除表
* 4.向表中插入数据
* 5.删除数据
* */
public class HBaseTest {

    static Configuration config = null;
    private Connection connection = null;
    private Table table = null;

    //1.hbase要连接zookeeper，需要配置zookeeper信息
    @Before
    public void init() throws Exception{
        config = HBaseConfiguration.create();
        //1.1配置zookeeper的服务器地址
        config.set("hbase.zookeeper.quorum","localhost");
        //1.2配置zookeeper端口
        config.set("hbase.zookeeper.property.clientPort", "2181");
        //1.3获取连接
        connection = ConnectionFactory.createConnection(config);
        //1.4获取表的引用
        table = connection.getTable(TableName.valueOf("user"));
    }

    //2.创建表的步骤
    @Test
    public void createTable() throws IOException {
        //2.1创建表的管理类
        HBaseAdmin admin = new HBaseAdmin(config);
        //2.2创建表的描述类
        TableName tableName = TableName.valueOf("test3");
        HTableDescriptor desc = new HTableDescriptor(tableName);
        //2.3创建列族描述类,将列族添加到表中
        HColumnDescriptor family = new HColumnDescriptor("info");
        desc.addFamily(family);
        HColumnDescriptor family2 = new HColumnDescriptor("info2");
        desc.addFamily(family2);
        //2.4创建表
        admin.createTable(desc);
    }

    //3.删除表
    @Test
    public void deleteTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(config);
        admin.disableTable("test3");
        admin.deleteTable("test3");
        admin.close();
    }

    //4.插入数据
    @Test
    public void insertData() throws IOException {
        Put put = new Put(Bytes.toBytes("xxx"));
        put.add(Bytes.toBytes("info1"),Bytes.toBytes("name"),Bytes.toBytes("xxx"));
        put.add(Bytes.toBytes("info1"),Bytes.toBytes("age"),Bytes.toBytes(1));
        put.add(Bytes.toBytes("info1"),Bytes.toBytes("sex"),Bytes.toBytes(1.1));
        put.add(Bytes.toBytes("info2"),Bytes.toBytes("sex"),Bytes.toBytes(2222));
        table.put(put);
    }

    //5.删除数据(5.1,5.2)
    @Test
    public void deleteData() throws IOException {
        //5.1创建删除的rowkey
        Delete delete = new Delete(Bytes.toBytes("xxx"));
        //5.3打开下边的语句可以指定只删除某个列族
        //delete.addFamily(Bytes.toBytes("info1"));
        //5.4打开下边的语句可以指定删除列族中的某一列,例如下边会删除info1列族下边的name列
        delete.addColumn(Bytes.toBytes("info1"),Bytes.toBytes("name"));
        //5.2删除操作
        table.delete(delete);
    }

    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }
}
