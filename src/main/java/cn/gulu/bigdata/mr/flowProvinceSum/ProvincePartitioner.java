package cn.gulu.bigdata.mr.flowProvinceSum;

import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.mr.flowProvinceSum
 * @ClassName: ProvincePartitioner
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-2 下午2:59
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-2 下午2:59
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 重写Partitioner类中的getPartition方法，目的就是为了让同一个省份的手机号进入同一个reducetask中
* 这里实现的细节就是将手机号的前三位作为手机省份的区分，并存储在字典中，0,1,2,3,4代表不同的省份
* 手机号进来之后就会返回0,1,2,3,4中的某个数，根据这个数字就会被发往不同的reducetask
* */

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    public static HashMap<String,Integer> provinceDict = new HashMap<String, Integer>();
    static {
        provinceDict.put("138",0);
        provinceDict.put("139",1);
        provinceDict.put("140",2);
        provinceDict.put("141",3);
    }


    @Override
    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
        String prefix = key.toString().substring(0,3);
        Integer provinceId = provinceDict.getOrDefault(prefix,4);

        return provinceId;
    }
}
