package model;

import java.util.ArrayList;
import java.util.LinkedList;



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
	 * waiting time estimation for the queue
	 * @return
	 */
	public double waittimeEstimating(){
		uti = getUti();
		System.out.println("uti for queue "+priority+" , "+uti);
		double pm = waitProb();
		System.out.println("wait pro  "+priority+" , "+pm);
		double t1 = 0;
		if (meanserv == 0 || uti == 1){
			System.out.println("mean serve frequency is 0 or the util is 1");
		}
		t1 = pm/(meanserv*(1-uti));
		double cs = DataCollection.cv(ServicePt, meanserv);
		double ca = DataCollection.cv(ArrivalPt, meanarrv);
		double tt = 2*numChannel;
		double t2 = (ca+cs)/tt;
//	    DecimalFormat formatter = new DecimalFormat("#0.000");
	    double result = t1*t2*1000000;
//	    result += getExeLatency();
//	    result = Double.valueOf(formatter.format(result));

		return result;
	}
	
	
	/**
	 * calculate the probability the data need to wait for processing
	 * @return
	 */
	public double waitProb(){
		if (uti>=0.7){
			return ((Math.pow(uti, numChannel)+uti)/2);
		}
		else{
			double t = (double)(numChannel+1)/2;
			return Math.pow(uti, t);
		}
	}
	
	
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
		
		return meanarrv/(numChannel*meanserv);
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
