import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class Main {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInt("K",10);
        try{
            String[] otherArgs = new String[]{"hdfs://localhost:9000/knn/input/","hdfs://localhost:9000/knn/output/"};
            Job job = new Job(conf,"KNN");
            job.setJarByClass(Main.class);
            // 设置map,combine,reduce
            job.setMapperClass(Map.class);
            job.setCombinerClass(Combine.class);
            job.setReducerClass(Reduce.class);
            // 设置map输出类型
            job.setMapOutputKeyClass(Pair.class);
            job.setMapOutputValueClass(Text.class);
            // 设置reduce输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // 设置输入和输出目录
            System.out.println(otherArgs[0]);
            System.out.println(otherArgs[1]);
            FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(otherArgs[1]));
            job.addCacheFile(new Path("hdfs://127.0.0.1:9000/knn/trainData/star_classification-sample-10000.csv").toUri());
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}