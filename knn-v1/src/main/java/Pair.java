import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
* 在排序问题上，使用mapreduce自带的排序是非常方便的，这里使用测试数据id和距离来创建自定义数据类型，
* 不仅可以实现按距离排序，还可以将同一个测试数据的数据集中在一起，同时因为结果是要和验证文件来比较的，
* 所以在使用测试数据行号作为id：
* */

public class Pair implements WritableComparable<Pair>{
    /*测试数据ID*/
    String type;
        /*距离*/
    Double dis;
    /*构造函数*/
    public Pair(){}
    public Pair(String type,Double dis){
        this.type=type;
        this.dis=dis;
    }

    @Override
    public int compareTo(Pair pair) {
        return Double.compare(dis,pair.dis);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeDouble(dis);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        type = dataInput.readUTF();
        dis = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o){
        Pair i = (Pair)o;
        return i.compareTo(this)==0;
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public String toString() {
        return "Pair{" +
                "type=" + type +
                ", dis=" + dis +
                '}';
    }
}