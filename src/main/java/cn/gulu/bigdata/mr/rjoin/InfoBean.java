package cn.gulu.bigdata.mr.rjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ProjectName: STBigData
 * @Package: cn.gulu.bigdata.mr.rjoin
 * @ClassName: InfoBean
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-5-4 下午4:20
 * @UpdateUser: 更新者
 * @UpdateDate: 19-5-4 下午4:20
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */

/*
* 1.为什么要实现Writable接口
* 2.为什么一定要有空参构造方法
* 3.write函数作用
* 4.readFields函数作用
* */

//1.如果对象要进行序列化，就必须实现Writable接口，并实现write和readFields接口
public class InfoBean implements Writable {

    private int order_id;
    private String dateString;
    private String p_id;
    private int amount;
    private String pname;
    private int category_id;
    private float price;

    //flag=0表示这个对象封装的是订单表记录
    //flag=1表示这个对象封装的是产品信息记录
    private String flag;

    //2.如果要进行序列化，必须要有空参构造方法
    public InfoBean() { }

    public void set(int order_id, String dateString, String p_id, int amount, String pname, int category_id, float price, String flag) {
        this.order_id = order_id;
        this.dateString = dateString;
        this.p_id = p_id;
        this.amount = amount;
        this.pname = pname;
        this.category_id = category_id;
        this.price = price;
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "InfoBean{" +
                "order_id=" + order_id +
                ", dateString='" + dateString + '\'' +
                ", p_id='" + p_id + '\'' +
                ", amount=" + amount +
                ", pname='" + pname + '\'' +
                ", category_id=" + category_id +
                ", price=" + price +
                ", flag='" + flag + '\'' +
                '}';
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    //3.write是将对象进行序列化的函数
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeUTF(dateString);
        out.writeUTF(p_id);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeInt(category_id);
        out.writeFloat(price);
        out.writeUTF(flag);
    }

    //4.readFields是反序列化函数
    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readInt();
        this.dateString = in.readUTF();
        this.p_id = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
        this.category_id = in.readInt();
        this.price = in.readFloat();
        this.flag = in.readUTF();
    }
}
