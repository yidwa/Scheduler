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
		temp = temp/values.size()/(mean*mean);
//		System.out.println("result is "+ String.valueOf(temp));
		
		return temp-1.0;
	}
	
}
