package cn.gulu.bigdata.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
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
* 6.单条查询
* 7.全表扫描
* 8.全表扫描，带有where条件
* 9.全表扫描，带有前缀过滤器
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


    //6.单条查询
    @Test
    public void queryData() throws IOException {
        //6.1 创建封装查询条件的类
        Get get = new Get(Bytes.toBytes("xxx"));
        //6.2 创建查询，指定列族和列名
        Result result = table.get(get);
        byte[] sex = result.getValue(Bytes.toBytes("info2"),Bytes.toBytes("sex"));
        System.out.println(Bytes.toInt(sex));
    }

    //7.扫描(7.1,7.2)
    @Test
    public void scanData() throws IOException {
        //7.1设置全表扫描封装类
        Scan scan = new Scan();
        //7.2扫描
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            byte[] name = result.getValue(Bytes.toBytes("info1"),Bytes.toBytes("name"));
            System.out.println(Bytes.toString(name));
        }
    }

    //8.全表扫描过滤器(8.1,8.2)
    @Test
    public void scanDataByFilter1() throws IOException {
        //8.1设置查询条件
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("info1"),Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,Bytes.toBytes("zhangsan"));
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        //8.2全表扫描
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            byte[] name = result.getValue(Bytes.toBytes("info1"),Bytes.toBytes("name"));
            System.out.println(Bytes.toString(name));
        }
    }

    //9.设置前缀过滤器
    @Test
    public void scanDataByFilter2() throws IOException {
        Filter f = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("^xx"));
        Scan scan = new Scan();
        scan.setFilter(f);
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            byte[] sex = result.getValue(Bytes.toBytes("info1"),Bytes.toBytes("sex"));
            System.out.println(Bytes.toInt(sex));
        }
    }

    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }
}
