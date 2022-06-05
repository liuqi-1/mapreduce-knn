import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;
import java.util.Map;

public class Reduce extends Reducer<Text, Pair,Text, Text> {
    private Text result = new Text();
    int k;
    int allCnt = 0;
    int wrongCnt=0;
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf =context.getConfiguration();
        k=conf.getInt("K",5);
    }
    @Override
    protected void reduce(Text key, Iterable<Pair> value, Context context) throws IOException, InterruptedException {
        PriorityQueue<Pair1> treemap = new PriorityQueue<>();
        int count = 0;
        for (Pair pair : value) {
            treemap.add(new Pair1(pair.dis, pair.type));
        }
        Map<String, Integer> map = new HashMap<>();
        while (!treemap.isEmpty() && count < k) {
            Pair1 pair = treemap.poll();
            if (map.containsKey(pair.type)) {
                int temp = map.get(pair.type);
                map.put(pair.type, temp + 1);
            } else {
                map.put(pair.type, 1);
            }
        }
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
        String tmpvalue=key.toString();
        int spilt=tmpvalue.indexOf(":");
        key.set(tmpvalue.substring(0,spilt));
        String real_type = tmpvalue.substring(spilt+1);
        result.set(label+","+real_type);  //输出为 <key=测试数据行号，value=label,real label>
        context.write(key,result);
        allCnt++;
        if(!label.equals(real_type))
            wrongCnt++;
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("正确率是："+(1-wrongCnt/(double)allCnt)*100+"%");
        context.write(new Text("正确率是："+(1-wrongCnt/(double)allCnt)*100+"%"),new Text(""));
    }
}