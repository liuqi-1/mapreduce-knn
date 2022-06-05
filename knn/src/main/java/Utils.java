public class Utils {
    /*计算欧式距离*/
    public static double getDistance1(String record1,String record2){
        String[] tokens1 = record1.split(",");
        String[] tokens2 = record2.split(",");
        double dis = 0;
        for(int i=1;i<tokens1.length-1;i++){
            Double d1 = Double.valueOf(tokens1[i]);
            Double d2 = Double.valueOf(tokens2[i]);
            dis += Math.pow(d1-d2,2);
        }
        return Math.sqrt(dis);
    }
    /*计算曼哈顿距离*/
    public static double getDistance2(String record1,String record2){
        String[] tokens1 = record1.split(",");
        String[] tokens2 = record2.split(",");
        double dis = 0;
        for(int i=1;i<tokens1.length-1;i++){
            Double d1 = Double.valueOf(tokens1[i]);
            Double d2 = Double.valueOf(tokens2[i]);
            dis += Math.abs(d1-d2);
        }
        return (dis);
    }
}