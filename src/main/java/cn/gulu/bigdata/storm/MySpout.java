package cn.gulu.bigdata.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.storm
 * @ClassName: MySpout
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-27 下午9:50
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-27 下午9:50
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class MySpout extends BaseRichSpout {

    SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = collector;
    }

    //storm函数的while(true)部分，不停地调用nextTuple函数
    @Override
    public void nextTuple() {
        collector.emit(new Values("hello world zhangjiaqian"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("love"));
    }
}
