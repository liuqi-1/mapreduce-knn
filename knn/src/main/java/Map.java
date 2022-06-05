import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Random;

public class Map extends Mapper<LongWritable,Text,Pair,Text> {
    HashSet<String> trainData = new HashSet<>(); // 训练数据
    Text outputVal=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            Integer pair_id=Integer.valueOf(tokens[0]);
            String pair_real_type=tokens[tokens.length - 1];
            /*循环计算该测试数据和训练数据之间的距离*/
            for (String trainDatum : trainData) {
                /*计算欧氏距离*/
                //double dis = Utils.getDistance1(value.toString(), trainDatum);
                /*计算曼哈顿距离*/
                double dis = Utils.getDistance2(value.toString(), trainDatum);
                /*拿到训练数据的类型*/
                String[] tokens_ = trainDatum.split(",");
                String type = tokens_[tokens_.length - 1];
                outputVal.set(type);
                /*创建Pair对象，并输出*/
                Pair pair = new Pair(pair_id,pair_real_type,dis);
                context.write(pair, outputVal);
        }
    }
}

/*
* 数据格式要求：第一列为行号，最后一列为类型，类型用Integer表示
* */