import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Combine extends Reducer<Pair, Text, Pair,Text> {
    static int k;
    int count;
    Integer curr_testid;
    Text outputVal = new Text();

    /*初始化函数*/
    public void setup(Context context) {
        Configuration conf=context.getConfiguration();
        k=conf.getInt("K",5);
        curr_testid=null;
        count=0;
    }

    @Override
    protected void reduce(Pair key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        if(curr_testid==null){
            curr_testid=key.test_id;
        }
        if(curr_testid!=key.test_id){
            count=0;
            curr_testid=key.test_id;
       }
        if(count!=k){
            for(Text type:value){
                count++;
                outputVal.set(key.dis+":"+type);
                key.dis= Double.valueOf(key.test_id);
                context.write(key, outputVal);
                if(count==k)
                    break;
            }
        }
    }
}