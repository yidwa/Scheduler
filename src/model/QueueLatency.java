package model;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;

import general.Methods;



public class QueueLatency {
	//  the list of service rate for 10 records
	public ArrayList<Double> ServicePt;
	//  the list of arrival rate for 10 records
	public ArrayList<Double> ArrivalPt;
	// the utilization of the component queue, every single component is a G/G/M queue
	public double uti;
	public double meanserv;
	public double meanarrv;
	public int numChannel;
	public int priority;
	
	public QueueLatency(LinkedList<Double> serv, LinkedList<Double> arrv, int numChannel, int priority) {
		// TODO Auto-generated constructor stub
		
		this.uti = 0.0;
		this.ServicePt = updateRates(serv);
		this.ArrivalPt = updateRates(arrv);
		this.meanarrv = 0.0;
		this.meanserv = 0.0;
		this.numChannel = numChannel;
		this.priority = priority;
	}
	
	public ArrayList<Double> updateRates(LinkedList<Double> temp){
		ArrayList<Double> result = new ArrayList<Double>();
		for(Double d :temp){
			result.add(d);
		}
		return result;
	}

	/**
	 * estimate the execution time according to the proposed scheduling scheme, for evaluate the qos violation
	 * @param host
	 * @param prohost
	 * @return
	 */
	public double exetimeEstimation(ArrayList<String> host, ArrayList<String> prohost){
		double result = 0.0;
		Double serlatest = ServicePt.get(ServicePt.size()-1);
		
		return result;
	}
	
	/**
	 * waiting time estimation for the queue
	 * @return
	 */
	public double waittimeEstimating(int numchannel){
		uti = getUti();

		double result_b = 0.0;
		double t11 = 0;
		double cs = 0;
		double ca = 0;
		double tt = 0;
		double t2 = 0;
		
		if (meanserv == 0 || uti == 1){
			System.out.println("mean serve frequency is 0 or the util is 1");
		
		}
		else{
			//update the waiting time formula
			t11 = uti/(meanserv*(1-uti));
			cs = DataCollection.cv(ServicePt, meanserv);
		    ca = DataCollection.cv(ArrivalPt, meanarrv);
		    tt = 2*numchannel;
			t2 = (ca+cs)/tt;

			result_b = t11*t2;
		
			result_b = Double.valueOf(Methods.formatter.format(result_b));
		}
		if(result_b<0){
			System.out.println("the uti "+uti+ " should be larger than 1, need to increase the number of channel");
		}
//		System.out.println("inside queue latency wait time estimation, uti "+uti+" , cs, cs "+cs+" , "+ca+" , "+"t11 "+t11+" , wait time "+result_b+ " ms");
	    return Math.abs(result_b);
	}
	
	
//	/**
//	 * calculate the probability the data need to wait for processing
//	 * @return
//	 */
//	public double waitProb(){
//		if (uti>=0.7){
//			return ((Math.pow(uti, numChannel)+uti)/2);
//		}
//		else{
//			double t = (double)(numChannel+1)/2;
//			return Math.pow(uti, t);
//		}
//	}
//	
	
	/**
	 *  get the utilization of the priority queue
	 * @return
	 */
	public double getUti(){
	
		if(!ArrivalPt.isEmpty()&&!ServicePt.isEmpty()){
			 meanserv = DataCollection.Mean(ServicePt);
			 meanarrv = DataCollection.Mean(ArrivalPt);
		}
		if (meanserv == 0 || numChannel == 0)
			return 0;
		
		double d;
		if(meanarrv == 0)
			 d = 0;
		else
			 d = 1.0/meanarrv;
		Double d1  =  d;
		Double d2 =  numChannel*(1.0/meanserv);
		if(d2 == 0)
			return 0;
		else
    		return Double.valueOf(Methods.formatter.format(d1/d2));
	}

	public int getNumChannel() {
		return numChannel;
	}

	public void setNumChannel(int numChannel) {
		this.numChannel = numChannel;
	}

	public ArrayList<Double> getServicePt() {
		return ServicePt;
	}

	public void setServicePt(ArrayList<Double> servicePt) {
		ServicePt = servicePt;
	}

	public ArrayList<Double> getArrivalPt() {
		return ArrivalPt;
	}

	public void setArrivalPt(ArrayList<Double> arrivalPt) {
		ArrivalPt = arrivalPt;
	}
	
	
}
