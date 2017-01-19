package model;

/*
 * the metric of throughput and latency for each topology
 */
public class Metrics {
	public String tname;
	public long throughput;
	public double latency;
	
	public Metrics(String tname, long throughput, double latency){
		this.tname = tname;
		this.throughput = throughput;
		this.latency = latency;
	}

	public long getThroughput() {
		return throughput;
	}

	public void setMetrics(long throughput, double latency) {
		this.throughput = throughput;
		this.latency = latency;
	}

	public double getLatency() {
		return latency;
	}



	@Override
	public String toString() {
		return tname + ", throughput=" + throughput + ", latency=" + latency;
	}
	
	
}
