import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;
import java.util.Map;

public class Reduce extends Reducer<Pair, Text,Text, Text> {
    private final Text result = new Text();
    int k;
    int count;
    Integer curr_testid;
    String curr_test_real_type;
    int allCnt = 0;
    int wrongCnt = 0;
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf =context.getConfiguration();
        k=conf.getInt("K",5);
        curr_testid=null;
        curr_test_real_type=null;
        count=0;
    }
    private class  Pair1 implements Comparable<Pair1> {
        double dis=0;
        String type;
        public Pair1(double dis, String type) {
            this.dis = dis;
            this.type = type;
        }
        @Override
        public int compareTo(Pair1 o) {
            return Double.compare(dis,o.dis);
        }
    }

    @Override
    protected void reduce(Pair key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        //TreeMap<Double, String> treemap = new TreeMap<>();
        PriorityQueue<Pair1> treemap = new PriorityQueue<>();
        /*将<dis,type>对放到treemap中进行排序*/
        for (Text pair : value) {
            String[] str = pair.toString().split(":");
            treemap.add(new Pair1(Double.parseDouble(str[0]),str[1]));
        }
        /*取出前k个，统计type出现的次数*/
        int count=0;
        Map<String, Integer> map = new HashMap<>();
        while (!treemap.isEmpty()&& count < k) {
            Pair1 p = treemap.poll();
            if (map.containsKey(p.type)) {
                int temp = map.get(p.type);
                map.put(p.type, temp + 1);
            } else {
                map.put(p.type, 1);
            }
            count++;
        }
        /*统计出现次数最多的type*/
        Iterator <String> it1=map.keySet().iterator();
        String label=it1.next();
        int num=map.get(label);
        while (it1.hasNext()){
            String currlabel=it1.next();
            if(num<map.get(currlabel)){
                label=currlabel;
                num=map.get(label);
            }
        }
        context.write(new Text(key.real_type),new Text(label));
        allCnt++;
        if(!key.real_type.equals(label))
            wrongCnt++;
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("正确率是："+(1-wrongCnt/(double)allCnt)*100+"%");
        context.write(new Text("正确率是："+(1-wrongCnt/(double)allCnt)*100+"%"),new Text(""));
    }
}