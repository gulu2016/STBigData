package cn.gulu.bigdata.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.storm
 * @ClassName: StormWordCount
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-27 下午8:59
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-27 下午8:59
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 问题：（待解决）为什么执行之后，什么输出都没有
* */
public class StormWordCount {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        //1.准备一个TopologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout",new MySpout(),1);
        topologyBuilder.setBolt("mybolt1",new MySplitBolt(),10).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("mybolt2",new MyCountBolt(),2).fieldsGrouping("mybolt1",new Fields("word"));


        //2.创建一个configuration,用来指定当前topology需要的worker数量
        Config config = new Config();
        config.setNumWorkers(2);


        //3.提交任务,分为集群模式和本地模式
        //StormSubmitter.submitTopology("mywordcount",config,topologyBuilder.createTopology());

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mywordcount",config,topologyBuilder.createTopology());
    }
}
