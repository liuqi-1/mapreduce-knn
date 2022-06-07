package knn.base.methods;

import java.io.*;
import java.util.*;

public class MainTest {
	public static int DataLength = 1000;
	public static Point[] pointArray = new Point[DataLength];
	public static Point[] originArray=new Point[DataLength];

	public static void main(String[] args) {
		long startTime=System.currentTimeMillis();
	   	String trainFilePath = "E://other//knnClassifier-master//src//knn//base//methods//design-data-1.txt";
	   	MainTest test = new MainTest();
	   	test.initPoints(trainFilePath);
	   	int index = 0, errorTotal = 0;

	   	originArray=pointArray;
	   	while (index<pointArray.length){
	   	  	String knnResult=pointArray[index].getKnnClassifier(10,pointArray,pointArray[index]);// classify by KNN-nearest
	   	  	System.out.println("predicted label : "+knnResult+" , actual label : "+originArray[index].getLabel());
		   	if(!Objects.equals(originArray[index].getLabel(), knnResult)){
				   errorTotal++;
	   	  	}
         	index++;
	   	}
	   	System.out.println("\n"+"The error number of total misclassified samples is "+errorTotal+"\n");
	   	System.out.println("accuracy: "+(pointArray.length-errorTotal+0.0)/(pointArray.length+0.0));
		long endTime=System.currentTimeMillis(); //获取结束时间
		System.out.println("程序运行时间： "+(endTime-startTime)+" ms");
	}

	public void initPoints(String filePath) {
		//get data from txt file
		try {
			FileInputStream fstream = new FileInputStream(filePath);
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			int number = 0;
			int count=0;
			//Read File Line By Line
			while ((strLine = br.readLine()) != null) {
				Scanner s = new Scanner(strLine);
				count=0;
				Point temp = new Point();
				while (s.hasNext()) { // Read line word by word
					String sample = s.next();
					if(count==0)
						temp.setLabel(sample);
					else if(count==1)
						temp.setF1(new Double(sample));
					else if(count==2)
						temp.setF2(new Double(sample));
					else if(count==3)
						temp.setF3(new Double(sample));
					else if(count==4)
						temp.setF4(new Double(sample));
					else if(count==5)
						temp.setF5(new Double(sample));
					else if(count==6)
						temp.setF6(new Double(sample));
					else if(count==7)
						temp.setF7(new Double(sample));
					else if(count==8)
						temp.setF8(new Double(sample));
					else temp.setF9(new Double(sample));
					count++;
				}
				pointArray[number] = temp;
				number++;
			}
			//Close the input stream
			fstream.close();
			in.close();

		} catch (Exception e) {//Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
	}
}
