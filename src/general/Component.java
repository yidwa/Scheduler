package general;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.A;

import model.DataCollection;

public class Component {
	// component id 
	public String cid;
	// the number of executors for the given component
	public long thread;
	// the executor information and the corresponding thread 
	public HashMap<Executor,ComponentThread> exe_thread;
	// the collection of component thread
	public HashMap<String,ComponentThread> threads;
	// the collection of active executors for this component
	public ArrayList<Executor> executors;
	// if the component is the spout
	public boolean spout;
	// if the component is the end/ output
	public boolean is_last;
	//the estimated total amount of tuples processed by this component and it's the emit amount for the spout
	public double totalprocess;
	// the last observation of amount transferred
	public long lasttrans;
	// the last observation of amount processed
	public long lastproc;
	// ! need to check if it's still needed
	public long execute;
	//  the list of service rate for 10 records
	public ArrayList<Double> ServicePt;
	//  the list of arrival rate for 10 records
	public ArrayList<Double> ArrivalPt;
	// the execute latency = process latency + waiting time
	public double exeLatency;
	// the process latency
	public double procLatency;
	// the mean service time for the past 10 periods
	double meanserv;
	// the mean arrival time for the past 10 periods
	double meanarrv;
	// the utilization of the component queue, every single component is a G/G/M queue
	public double uti;
	// the estimated latency based on queuing theory
	public double modellatency;

	Map<String,String> valuealltime;
	// component with one upstream node and one downstream node
	public Component(String cid, long thread){
		this.cid = cid;
		this.thread = thread;
		this.spout = false;
		this.is_last = false;
		this.threads = new HashMap<String, ComponentThread>();
		this.lastproc = 0;
		this.lasttrans = 0;
		this.executors = new ArrayList<Executor>();
		this.ServicePt = new ArrayList<Double>();
		this.ArrivalPt = new ArrayList<Double>();
		this.meanarrv = 0;
		this.meanserv = 0;
		this.uti = 0;
		this.valuealltime = new HashMap<String, String>();
		// may need to change to min(thrad, executoroftopology)
		// this.numserver = thread;
		}
	

	// component either is a starting point or ending point
	public Component(String cid, long thread, Boolean spout){
		this.cid = cid;
		this.thread = thread;
		if(spout == true){
			this.spout = true;
			this.is_last = false;
			}
		else{
			this.spout = false;
			this.is_last = true;
			}
		this.threads = new HashMap<String, ComponentThread>();
		this.executors = new ArrayList<Executor>();
		this.ServicePt = new ArrayList<Double>();
		this.ArrivalPt = new ArrayList<Double>();
		this.meanarrv = 0;
		this.meanserv = 0;
		this.uti = 0;
		//this.numserver = thread;
	}

    /**
     * The estimated amount of tuple processed by its threads, and update the processed amount 
     * @param numincoming
     * @return
     */
	public double totalProcessed(double numincoming){
		//update the arrival rate
		updateArr_Ser(numincoming,true);
		double amount = 0;
		double temp = 0;
		for(String ctt : threads.keySet()){
			temp = threads.get(ctt).threadProcessed(numincoming);
			amount += temp;
			}
		totalprocess = Double.valueOf(Methods.formatter.format(amount));
		return totalprocess;
	}
		
	
	/**
	 * Update the arrival rate and service rate 
	 * @param numincoming
	 * @param arr , true is to update the arrival rate, false is to update the service rate
	 */
	public void updateArr_Ser(Double numincoming, boolean arr){
		ArrayList<Double> temp = new ArrayList<Double>();
		if(arr == true)
			temp = getArrv();
		else 
			temp = getServ();
		
		if(temp.size() == 10){
			temp.remove(0);
		}
		temp.add(numincoming);
		if( arr == true){
			setArrv(temp);
		}
		else{ 
			setServ(temp);	
		}
	}


	/**
	 *  the transformation rate between transferred and processed
	 * @param lp
	 * @param lt
	 * @return
	 */
	public long procTotran(long lp, long lt){
		long trans = 0;
		if(lp!=0){
			trans = (long)(lt * totalprocess/ lp);
			return trans;
		}
		else{
			return 0;
		}
	
	}
		
	/**
	 * estimate the waiting time
	 * @return the execution time
	 */
	public double waittimeEstimating(){
		uti = getUti();
		System.out.println("uti "+uti);
		double pm = waitProb();
		System.out.println("wait pro "+pm);
		double t1 = 0;
		if (meanserv == 0 || uti == 1){
			System.out.println("mean serve frequency is 0 or the util is 1");
		}
		t1 = pm/(meanserv*(1-uti));
		double cs = DataCollection.cv(ServicePt, meanserv);
		double ca = DataCollection.cv(ArrivalPt, meanarrv);
		double tt = 2*thread;
		double t2 = (ca+cs)/tt;
	    DecimalFormat formatter = new DecimalFormat("#0.000");
	    double result = t1*t2*1000000;
	    result += getExeLatency();
	    result = Double.valueOf(formatter.format(result));
	    setModellatency(result);
		return result;
	}
	
	/**
	 *  get the utilization of the component queue
	 * @return
	 */
	public double getUti(){
		if(!ArrivalPt.isEmpty()&&!ServicePt.isEmpty()){
			 meanserv = DataCollection.Mean(ServicePt);
			 meanarrv = DataCollection.Mean(ArrivalPt);
		}
		if (meanserv == 0 || thread == 0)
			return 0;
		return meanarrv/(thread*meanserv);
	}
	
	/**
	 * calculate the probability the data need to wait for processing
	 * @return
	 */
	public double waitProb(){
		if (uti>=0.7){
			return ((Math.pow(uti, thread)+uti)/2);
		}
		else{
			double t = (double)(thread+1)/2;
			return Math.pow(uti, t);
		}
	}
	
	

	public long getLasttrans() {
		return lasttrans;
	}

	public void setLast(long lastproc, long lasttrans) {
		this.lasttrans = lasttrans;
		this.lastproc = lastproc;
	}

	public long getLastproc() {
		return lastproc;
	}
		
	@Override
	public String toString() {
		return "name=" + cid + ", thread=" + thread + ",";
	}
	
	
	public ArrayList<Executor> getExecutors() {
		return executors;
	}

	public double getModellatency() {
		return modellatency;
	}

	public void setModellatency(double modellatency) {
		this.modellatency = modellatency;
	}


	public void setLasttrans(long lasttrans) {
		this.lasttrans = lasttrans;
	}

	public double getTotalprocess() {
		return totalprocess;
	}

	public void setTotalprocess(double totalprocess) {
		this.totalprocess = totalprocess;
	}
	public void setExecutors(ArrayList<Executor> executors) {
		this.executors = executors;
	}

	public HashMap<String,ComponentThread> getThreads() {
		return threads;
	}

	public void setThreads(HashMap<String,ComponentThread> threads) {
		this.threads = threads;
	}


	public ArrayList<Double> getServ() {
		return ServicePt;
	}


	public void setServ(ArrayList<Double> serv) {
		this.ServicePt = serv;
	}

	public ArrayList<Double> getArrv() {
		return ArrivalPt;
	}

	public void setArrv(ArrayList<Double> arrv) {
		this.ArrivalPt = arrv;
	}

	public double getMeanserv() {
		return meanserv;
	}


	public void setMeanserv(double meanserv) {
		this.meanserv = meanserv;
	}

	public double getMeanarrv() {
		return meanarrv;
	}

	public void setMeanarrv(double meanarrv) {
		this.meanarrv = meanarrv;
	}

	public void setUti(double uti) {
		this.uti = uti;
	}

	public double getExeLatency() {
		return exeLatency;
	}

	public void setExeLatency(double exeLatency) {
		this.exeLatency = exeLatency;
	}
	
	public double getProcLatency() {
		return procLatency;
	}

	public void setProcLatency(double procLatency) {
		this.procLatency = procLatency;
	}


	public Map<String, String> getValuealltime() {
		return valuealltime;
	}


	public void setValuealltime(Map<String, String> valuealltime) {
		this.valuealltime = valuealltime;
	}
	
}
