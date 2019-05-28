package cn.gulu.bigdata.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.storm
 * @ClassName: MySplitBolt
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-28 上午10:22
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-28 上午10:22
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 问题：
* 1.MySplitBolt的组成部分(1-1,1-2)
* */
public class MySplitBolt extends BaseRichBolt {
    OutputCollector collector;
    //1-1.初始化方法
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    //1-2.被storm框架中while(true)调用的部分
    @Override
    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] arrWords = line.split(" ");
        for(String word:arrWords){
            collector.emit(new Values(word,1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","num"));
    }
}
