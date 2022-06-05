public class Pair1 implements Comparable<Pair1>{
    double dis;
    String type;
    public Pair1(double dis, String type) {
        this.dis = dis;
        this.type = type;
    }

    @Override
    public int compareTo(Pair1 o) {
        return Double.compare(dis, o.dis);
    }
}
