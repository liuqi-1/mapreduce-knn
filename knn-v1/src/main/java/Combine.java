import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeMap;

public class Combine extends Reducer<Text, Pair,Text,Pair> {
    int k;

    /*初始化函数*/
    public void setup(Context context) {
        Configuration conf=context.getConfiguration();
        k=conf.getInt("K",5);
    }
    @Override
    protected void reduce(Text key, Iterable<Pair> value, Context context)  {
        try {
            PriorityQueue<Pair1> treemap = new PriorityQueue<>();
            int count = 0;
            for (Pair pair : value) {
                treemap.add(new Pair1(pair.dis, pair.type));
            }
            while (!treemap.isEmpty() && count < k) {
                Pair1 pair1 = treemap.poll();
                count++;
                context.write(key, new Pair(pair1.type, pair1.dis));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}