package general;

import model.General;

public class ComponentThread {
	// the latest observation of execution time for each tuple
	public double executelatency;
	public double processlatency;

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

	

	public ComponentThread(String ctid) {
		this.ctid = ctid;
		this.prob = 0;
		this.totalexecute = 0;
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


	// processed amount by each thread
	public double threadProcessed(double amount){
		double est = 0;
		double result = 0;
		if (processlatency != 0){
			
			est = Double.valueOf(Methods.formatter.format(1000/Double.valueOf(processlatency)));
			
		}

//			System.out.println("history data not updated, please update data first");
		
		if(prob>0){
			double pred = (double) (prob * amount);
//			System.out.println("threadprocesssed "+ctid+" , "+amount+ " , "+prob +" , "+pred);
//			System.out.println("thread processed estimated amount  "+ est +" , predicted amount "+ pred);
			result = Math.min(est, pred);
		}
//		setRecevietotal(amount);
		return result;
	}
	
	
	public String getId() {
		return ctid;
	}



	public void setId(String ctid) {
		this.ctid = ctid;
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
