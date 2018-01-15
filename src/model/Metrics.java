package model;

/*
 * the metric of throughput and latency for each topology
 */
public class Metrics {
	public String tname;
//	public long throughput;
	public double thrratio;
	public double latency;
	
	public Metrics(String tname, long thrratio, double latency){
		this.tname = tname;
		this.thrratio = thrratio;
		this.latency = latency;
	}

	public double getThroughput() {
		return thrratio;
	}

	public void setMetrics(double thrratio, double latency) {
		this.thrratio = thrratio;
		this.latency = latency;
	}

	public double getLatency() {
		return latency;
	}



	@Override
	public String toString() {
		return tname + ", throughput ratio=" + thrratio + ", latency=" + latency;
	}
	
	
}
