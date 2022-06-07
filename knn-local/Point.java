package knn.base.methods;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Point {
	private String label;//实际标签
	private double f1;
	private double f2;
	private double f3;
	private double f4;
	private double f5;
	private double f6;
	private double f7;
	private double f8;
	private double f9;
	private String knnLabel;//预测的标签

	public String getKnnLabel() {
		return knnLabel;
	}

	public void setKnnLabel(String knnLabel) {
		this.knnLabel = knnLabel;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public void setF1(double f1) {
		this.f1 = f1;
	}

	public void setF2(double f2) {
		this.f2 = f2;
	}

	public void setF3(double f3) {
		this.f3 = f3;
	}

	public void setF4(double f4) {
		this.f4 = f4;
	}

	public void setF5(double f5) {
		this.f5 = f5;
	}

	public void setF6(double f6) {
		this.f6 = f6;
	}

	public void setF7(double f7) {
		this.f7 = f7;
	}

	public void setF8(double f8) {
		this.f8 = f8;
	}

	public void setF9(double f9) {
		this.f9 = f9;
	}

	public String getLabel() {
		return label;
	}

	public double getF1() {
		return f1;
	}

	public double getF2() {
		return f2;
	}

	public double getF3() {
		return f3;
	}

	public double getF4() {
		return f4;
	}

	public double getF5() {
		return f5;
	}

	public double getF6() {
		return f6;
	}

	public double getF7() {
		return f7;
	}

	public double getF8() {
		return f8;
	}

	public double getF9() {
		return f9;
	}

	public static Point getInstance(){
		return new Point();
	}

	private double getDistance(Point p1, Point p2){
		//return Math.sqrt((p1.f1-p2.f1)*(p1.f1-p2.f1) + (p1.f2-p2.f2)*(p1.f2-p2.f2)+(p1.f3-p2.f3)*(p1.f3-p2.f3)+(p1.f4-p2.f4)*(p1.f4-p2.f4)+(p1.f5-p2.f5)*(p1.f5-p2.f5)+(p1.f6-p2.f6)*(p1.f6-p2.f6)+(p1.f7-p2.f7)*(p1.f7-p2.f7)+(p1.f8-p2.f8)*(p1.f8-p2.f8)+(p1.f9-p2.f9)*(p1.f9-p2.f9));
		return Math.abs(p1.f1-p2.f1)+Math.abs(p1.f2-p2.f2)+Math.abs(p1.f3-p2.f3)+Math.abs(p1.f4-p2.f4)+Math.abs(p1.f5-p2.f5)+Math.abs(p1.f6-p2.f6)+Math.abs(p1.f7-p2.f7)+Math.abs(p1.f8-p2.f8)+Math.abs(p1.f9-p2.f9);
	}

	private Point[] getKnnPoints(int Knearest, Point[] tempArray, Point now) {
		Point tmp[]=tempArray;
		int cnt=0;
		Map<Point, Double> dislist=new HashMap<>();
		for (Point point : tempArray) {
			double dis = getDistance(point, now);
			dislist.put(point, dis);
		}
		Point[] ans =new Point[Knearest];
		for(int i=0;i<tmp.length;i++){
			for(int j=i+1;j<tmp.length;j++){
				if(dislist.get(tmp[i])>dislist.get(tmp[j])){
					Point t=tmp[i];
					tmp[i]=tmp[j];
					tmp[j]=t;
				}
			}
		}
		System.arraycopy(tmp, 0, ans, 0, Knearest);
		return ans;
	}

	public String getKnnClassifier(int kNearest, Point[] trainArray, Point now){
		Point[] kClassArray = getKnnPoints(kNearest,trainArray,now);
		int knnClassifer;
		int QSO = 0;
		int GALAXY = 0;
		int STAR = 0;
		for (Point point : kClassArray) {
			if (Objects.equals(point.label, "QSO")) QSO++;
			else if (Objects.equals(point.label, "STAR")) STAR++;
			else if (Objects.equals(point.label, "GALAXY"))GALAXY++;
		}

		if(QSO>=STAR&&QSO>=GALAXY){
			now.setKnnLabel("QSO");
			return "QSO";
		}

		else if(GALAXY>=STAR&&GALAXY>=QSO){
			now.setKnnLabel("GALAXY");
			return "GALAXY";
		}

		else{
			now.setKnnLabel("STAR");
			return "STAR";
		}
	}
}