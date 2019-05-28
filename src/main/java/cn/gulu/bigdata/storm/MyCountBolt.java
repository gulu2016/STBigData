package cn.gulu.bigdata.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.storm
 * @ClassName: MyCountBolt
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-28 上午10:38
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-28 上午10:38
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 问题：MyCountBolt的结构(1-1,1-2)
*
* */
public class MyCountBolt extends BaseRichBolt {
    OutputCollector collector;
    Map<String,Integer> map = new HashMap<String, Integer>();

    //1-1.初始化
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    //1-2storm的while(true)部分
    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer num = input.getInteger(1);

        System.out.println(Thread.currentThread().getId()+"   word:"+word);
        if(map.containsKey(word)){
            Integer count = map.get(word);
            map.put(word,count+num);
        }else
            map.put(word,num);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //没有输出
    }
}
