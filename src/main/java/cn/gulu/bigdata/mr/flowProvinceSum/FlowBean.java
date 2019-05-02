package cn.gulu.bigdata.mr.flowProvinceSum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.mr.flowsum
 * @ClassName: FlowBean
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-1 下午7:37
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-1 下午7:37
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long dFlow;
    private long sumFlow;

    //反序列化的时候需要反射调用空参构造函数，需要显示定义一个
    public FlowBean(){}

    public FlowBean(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow+dFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }


    @Override
    public int compareTo(FlowBean o) {
        //从大到小，当前对象要和比较对象比，当前对象大，返回-1
        return this.sumFlow>o.getSumFlow()?-1:1;
    }

    //序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dFlow);
        out.writeLong(sumFlow);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        dFlow = in.readLong();
        sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", dFlow=" + dFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }
}
