package general;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.A;

import model.DataCollection;

public class Component {
	public String cid;
	public long thread;
	public HashMap<Executor,ComponentThread> exe_thread;
//	public ArrayList<ComponentThread> threads;
	// for initial
//	public ArrayList<ComponentThread> threads;
	// for each update
	public HashMap<String,ComponentThread> threads;
	public ArrayList<Executor> executors;


	//	public String name;
	//prob of each thread
//	public ArrayList<Double> prob;
	//processed by each thread, update automatically by totalpreocessed
//	public ArrayList<Long> predproc;
	// execution time of each thread
//	public ArrayList<Double> executime;
//	public Component pre;
//	public Component  next;
	public boolean spout;
	public boolean is_last;
	//public ComponentThroughput throughput;
	//the total amount of tuples processed by this component
	public double totalprocess;
	// the last observation of amount transferred
	public long lasttrans;
//	public HashMap<String, Long> lasttrans;
	// the last observation of amount processed
//	public HashMap<String, Long> lastproc;
	public long lastproc;
	public long execute;
//	public double pro_tran;
	
	public long numserver;
	public ArrayList<Double> ServicePt;
	//for time estimation
	public double exeLatency;
	public ArrayList<Double> ArrivalPt;
	double meanserv;
	double meanarrv;
	public double uti;
	public double modellatency;

	// component with one up point and one down point
	public Component(String cid, long thread){
		this.cid = cid;
		this.thread = thread;
//		this.next = next;
		this.spout = false;
		this.is_last = false;
		this.threads = new HashMap<String, ComponentThread>();
//		this.threads = initialthreads();
		this.lastproc = 0;
		this.lasttrans = 0;
		this.executors = new ArrayList<Executor>();
//		this.predproc = new ArrayList<Long>();
//		this.pro_tran = 1;
		
		// may need to change to min(thrad, executoroftopology)
		this.numserver = thread;
		this.ServicePt = new ArrayList<Double>();
		this.ArrivalPt = new ArrayList<Double>();
		this.meanarrv = 0;
		this.meanserv = 0;
		this.uti = 0;
		}
	


	// component either starting point or ending point
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
		
		this.numserver = thread;
		this.ServicePt = new ArrayList<Double>();
		this.ArrivalPt = new ArrayList<Double>();
		this.meanarrv = 0;
		this.meanserv = 0;
		this.uti = 0;
//		this.predproc = new ArrayList<Long>();
	}

//	public ArrayList<ComponentThread> initialthreads(){
//		ArrayList<ComponentThread> t = new ArrayList<ComponentThread>();
//		for (int i = 0; i<thread; i++){
//			ComponentThread c = new ComponentThread(null);
//			t.add(c);
//		}
//		return t;
//	}
////	
//	public void updatethreads(HashMap<Executor,Double> proc, HashMap<Executor,Double> exeu){
//		ArrayList<Executor> exeutor = new ArrayList<Executor>();
////		exe = ;
//		ComponentThread ct;
//		for(int i=0; i<threads.size(); i++){
//			Executor e = exeutor.get(i);
//			ct = threads.get(i);
//			ct.prob = proc.get(e);
//			ct.executime = exeu.get(e);
////			exe_thread.put(e,ct);
//		}
//	}
//	
//	
	
	
//	public void updateP_T(long trans, long proc){
//	lasttrans = trans;
//	lastproc = proc;
//}
	
	//the sum of processed tuple by each thread, also update the processed amount list
	public double totalProcessed(double numincoming){
			updateArr_Ser(numincoming,true);
//			updatethreads(proc, exe);
			double amount = 0;
			double temp = 0;
//			System.out.println("calculating the totalprocessed for "+cid+" , with "+numincoming);
//			for (int i = 0 ; i<thread; i++){
			for(String ctt : threads.keySet()){
//				System.out.println("total processed "+cid+" , "+numincoming);
				
				temp = threads.get(ctt).threadProcessed(numincoming);
				
//				temp = Double.valueOf(Methods.formatter.format(temp));
//				predproc.add(temp);
//				System.out.println("thread processed of "+cid+","+ threads.get(ctt).ctid+" , "+temp);
				amount += temp;
//				System.out.println("thread "+ i +" amount "+temp);
			}
			totalprocess = Double.valueOf(Methods.formatter.format(amount));
//			System.out.println("total processed "+cid+","+totalprocess);
			return totalprocess;
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
	
	//true is update arr, false is update serv
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
		//	System.out.println("now the arrival is "+temp.toString());
		}
		else{ 
			setServ(temp);
//			setExeLatency(numincoming);
		//	System.out.println("now the service is "+temp.toString());
			
		}
	
		//test first if update arr changed
//		setArrv(arrtemp);
	}


	// the transformation between transferred and processed
//	public long procTotran(long lastproc, long lasttrans)
	public long procTotran(long lp, long lt){
//		System.out.println("pro tocong "+lp + " , "+lt);
		long trans = 0;
		trans = (long)(lt * totalprocess/ lp); 
//		lasttrans = trans;
//		lastproc = totalprocess;
//		if(lastproc!=0){
		if(lp!=0){
			return trans;
		}
		else{
			System.out.println("error! last processed is 0");
			return 0;
		}
	
	}
		
	public double waittimeEstimating(){
//		System.out.println("rate "+ServicePt.toString()+" , "+ArrivalPt.toString());
		uti = getUti();
		String temp="";
		double pm = waitProb();
		System.out.println("uti "+uti+ " , pm "+pm);
		double t1 = 0;
		if (meanserv == 0 || uti == 1){
			System.out.println("mean serve frequency is 0 or the util is 1");
		}
		
		
		t1 = pm/(meanserv*(1-uti));
		System.out.println("pm "+pm+" , uti"+uti+" , numberserver "+numserver);
		double cs = DataCollection.cv(ServicePt, meanserv);
		double ca = DataCollection.cv(ArrivalPt, meanarrv);
		System.out.println("cs "+cs + " ca "+ca+ " cs+ca "+ (ca+cs));
		double tt = 2*numserver;
		double t2 = (ca+cs)/tt;
		System.out.println("t1 is "+t1);
		System.out.println("t2 is "+t2+ " t1* t2 = "+ t1*t2);
	    DecimalFormat formatter = new DecimalFormat("#0.000");
//	    System.out.println(formatter.format(t1*t2));
	    double result = t1*t2*1000000;
//	    double result = Double.valueOf(formatter.format(t1*t2*1000000));
//	    System.out.println("estimation result "+result+" and the executelatency is "+ getExeLatency());
	    result += getExeLatency();
	    result = Double.valueOf(formatter.format(result));
//	    temp+= "uti " +uti+", ";
//	    temp+= "wait possibility " +pm+ ", ";
//	    temp+= "t1 "+ t1+" ,t2 "+t2+" result "+ result+"\n";
//	    Methods.writeFile(temp, "Results.txt");
	    setModellatency(result);
		return result;
	}
	
	public double getUti(){
	
		if(!ArrivalPt.isEmpty()&&!ServicePt.isEmpty()){
			 meanserv = DataCollection.Mean(ServicePt);
			 meanarrv = DataCollection.Mean(ArrivalPt);
			 System.out.println("mean serv "+ meanserv + " mean arr "+ meanarrv);
		}
		if (meanserv == 0 || numserver == 0)
			return 0;
		return meanarrv/(numserver*meanserv);
		
	}
	
	
	public double waitProb(){
		if (uti>=0.7){
			return ((Math.pow(uti, numserver)+uti)/2);
		}
		else{
			double t = (double)(numserver+1)/2;
			return Math.pow(uti, t);
		}
	}
	
	
	public void updateArr(ArrayList<Double> arr){
		ArrivalPt = arr;
	}
	public void updateServ(ArrayList<Double> serv){
	    ServicePt = serv;
	}
	
   public void updateSlotnum(long num){
	   numserver = num;
   }
//   
   public void updateData(ArrayList<Double> arr, ArrayList<Double> serv, long num){

	   updateArr(arr);
	   updateServ(serv);
	   updateSlotnum(num);
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

	
	
	
//	public void setLastproc(long lastproc) {
//		this.lastproc = lastproc;
//	}
		
	@Override
	public String toString() {
		return "name=" + cid + ", thread=" + thread + ",";
	}
	
	
	public ArrayList<Executor> getExecutors() {
		return executors;
	}



	public void setExecutors(ArrayList<Executor> executors) {
		this.executors = executors;
	}

	public long getExecute() {
		return execute;
	}

	public void setExecute(long execute) {
		this.execute = execute;
	}

	public HashMap<String,ComponentThread> getThreads() {
		return threads;
	}



	public void setThreads(HashMap<String,ComponentThread> threads) {
		this.threads = threads;
	}



	public long getNumserver() {
		return numserver;
	}



	public void setNumserver(long numserver) {
		this.numserver = numserver;
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
	
	
}
