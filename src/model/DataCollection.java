package model;

import java.util.ArrayList;

public class DataCollection {
//	public ArrayList<Double> arr;
//	public ArrayList<Double> serv;
//	public Double meanarr;
//	public Double meanserv;
//	public Double sdarr;
//	public Double sdserv;
//	public Double cvarr;
//	public Double cvserv;
//	
//	public DataCollection(){
//		this.arr = new ArrayList<Double>();
//		this.serv = new ArrayList<Double>();
//		this.meanarr = 
//		this.meanserv = 0.0;
//		this.sdarr = 0.0;
//		this.sdserv = 0.0;
//	}


	public static double Mean(ArrayList<Double> values){
		double sum = 0;
		if (!values.isEmpty()){
			for(Double d : values){
				sum+= d;
			}
			return sum/values.size();
		}
		return sum;
	}

	public static double StandardDeviation(ArrayList<Double> values, double mean){
		double sum = 0;
		if(!values.isEmpty()){
			for (Double d : values){
				sum += Math.pow(d-mean, 2);
			}
			return Math.sqrt(sum/values.size());
		}
		return sum;
	}
	
	public static double cv(ArrayList<Double> values, double mean){
		double temp = 0; 
		if (!values.isEmpty()){
			for(Double d : values){
				temp+=d*d;
			}
		}
		if(mean!=0){
			temp = temp/values.size()/(mean*mean);
//			System.out.println("cv result is "+ String.valueOf(temp));
		}
		return temp;
	}

//	public static void main(String[] args) {
//		ArrayList<Double> values = new ArrayList<>();
//		values.add(2.5);
//		values.add(2.4);
//		values.add(2.6);
//		double mean =2.0;
//		double d = DataCollection.cv(values, mean);
//		System.out.println("d is "+d);
//	}
}
