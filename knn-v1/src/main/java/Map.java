import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Random;

public class Map extends Mapper<LongWritable, Text,Text,Pair> {
    HashSet<String> trainData = new HashSet<>(); // 训练数据

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Pair>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        /*从缓存读取文件*/
        String path = context.getLocalCacheFiles()[0].getName();
        File itermOccurrenceMatrix = new File(path);
        FileReader fileReader = new FileReader(itermOccurrenceMatrix);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String s;
        while ((s = bufferedReader.readLine()) != null) {
            trainData.add(s);
            int i = new Random().nextInt()%10;
            if(i<6) trainData.add(s);
        }
        System.out.println("trainData size:" + trainData.size());
        bufferedReader.close();
        fileReader.close();
    }

    @Override
    protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, Pair>.Context context)
            throws IOException, InterruptedException {
        /*创建lineCnt:type格式key*/
        String[] tokens = value.toString().split(",");
        Text outputKey = new Text(tokens[0] + ":" + tokens[tokens.length - 1]);
        /*循环计算该测试数据和训练数据之间的距离*/
        for (String trainDatum : trainData) {
            /*计算欧氏距离*/
            //double dis = Utils.getDistance1(value.toString(), trainDatum);
            /*计算曼哈顿距离*/
            double dis = Utils.getDistance2(value.toString(), trainDatum);
            /*拿到训练数据的类型*/
            String[] tokens_ = trainDatum.split(",");
            String type = tokens_[tokens_.length - 1];
            /*创建Pair对象，并输出*/
            Pair pair = new Pair(type, dis);
            context.write(outputKey, pair);
        }
    }
}