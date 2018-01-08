package general;

import java.util.ArrayList;

import model.General;
//import model.MetricUpdate;
import opt.Optimisation;

public class ComponentThread {
	// the latest observation of execution time for each tuple
	public double executelatency;
	public double processlatency;
	public String cid;
	// probability that tuples come to this thread
	public double prob;
//	public String cid;
//	public long cport;
//	public long emit;
	
	public long execute;
	public long totalexecute;
	public long ack;
//	public double recevietotal;
	public long compoemit;

	public String changedtype;
	
	
	public long getCompoemit() {
		return compoemit;
	}


	public void setCompoemit(long compoemit) {
		this.compoemit = compoemit;
	}


	public Executor executor;
	public String ctid;
	
	

	//	public int index;


//	public final int TimeUnit = 30;

	

	public ComponentThread(String cid, String ctid) {
		this.cid = cid;
		this.ctid = ctid;
		this.prob = 0;
		this.totalexecute = 0;
		this.changedtype ="";
	}
	
	
	public void updateThread(long execute, double executelatency, double processlatency, long ack, Executor exe){
//		setEmit(emit);
		long deltaexecute = execute - totalexecute;
		setTotalexecute(execute);
		setExecute(deltaexecute);
		setExecutelatency(executelatency);
		setProcesslatency(processlatency);
		setAck(ack);
		setExecutor(exe);

	}
	

	public long getTotalexecute() {
		return totalexecute;
	}


	public void setTotalexecute(long totalexecute) {
		this.totalexecute = totalexecute;
	}


	/**
	 * estimate the processed amount by each thread 
	 * @param amount incoming amount of tuple
	 * @return
	 */
	public double threadProcessed(double amount){
		double est = 0;
		double result = 0;
		if (processlatency != 0){
			est = Double.valueOf(Methods.formatter.format(1000/Double.valueOf(processlatency)));
			}
		
		if(prob>0){
			double pred = (double) (prob * amount);
			result = Math.min(est, pred);
			
			String hostname = getExecutor().host;
			int ind;
			if(hostname.contains("s"))
				ind = -1;
			else if (hostname.contains("l"))
				ind = 1;
			else
				ind = 0;
			
			int updateindex = Optimisation.findType(ind, est, pred, processlatency);
			if(ind == updateindex){
				System.out.println(ctid+ " , "+getExecutor().getHost().substring(0, 1)+" unchanged " +ind+" , "+updateindex);
			}
			else{
				String s =getExecutor().getHost().substring(0, 1);
				
//				System.out.println(ctid+", "+s+" changed to "+Methods.findHosttype(s, updateindex-ind));
				setChangedtype(Methods.findHosttype(s, updateindex-ind));
			}
//			System.out.println(ctid+"thread processed "+(pred-est));
		}
		else{
			System.out.println("thread processed prob <0");
		}
//		setRecevietotal(amount);
		return result;
	}
	
	
	public String getChangedtype() {
		return changedtype;
	}


	public void setChangedtype(String changedtype) {
		this.changedtype = changedtype;
	}


	public String getId() {
		return ctid;
	}



	public void setId(String ctid) {
		this.ctid = ctid;
	}



	public String getCid() {
		return cid;
	}


	public long getExecute() {
		return execute;
	}



	public void setExecute(long execute) {
		this.execute = execute;
	}
	
	public Executor getExecutor() {
		return executor;
	}

	public long getAck() {
		return ack;
	}



	public void setAck(long ack) {
		this.ack = ack;
	}
	public double getExecutelatency() {
		return executelatency;
	}



	public void setExecutelatency(double executelatency) {
		this.executelatency = executelatency;
	}



	public double getProcesslatency() {
		return processlatency;
	}



	public void setProcesslatency(double processlatency) {
		this.processlatency = processlatency;
	}

	

	public void setExecutor(Executor executor) {
		this.executor = executor;
	}



	public double getProb() {
		return prob;
	}


	public void setProb(double prob) {
		this.prob = prob;
	}


	@Override
	public String toString() {
		return "ComponentThread [executelatency=" + executelatency + ", processlatency=" + processlatency + ", cid="
				+ cid + ", prob=" + prob + ", execute=" + execute + ", totalexecute=" + totalexecute + ", ack=" + ack
				+ ", compoemit=" + compoemit + ", changedtype=" + changedtype + ", executor=" + executor + ", ctid="
				+ ctid + "]";
	}

	
//	public long getEmit() {
//	return emit;
//	}
//
//
//	public void setEmit(long emit) {
//	this.emit = emit;
//	}

//	public long getCport() {
//		return cport;
//	}
//
//
//	public void setCport(long cport) {
//		this.cport = cport;
//	}


	
	
}
