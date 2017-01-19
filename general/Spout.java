package general;

public class Spout {
	String id;
	String latency;
	Long transfered;
	Long emitted;
	int executor;
	
	public Spout(String id){
		this.id = id;
		this.latency = "";
		this.transfered = (long) 0;
		this.emitted = (long) 0;
		this.executor = 0;
	}

	public int getExecutor() {
		return executor;
	}

	public void setExecutor(int executor) {
		this.executor = executor;
	}

	public Long getEmitted() {
		return emitted;
	}

	@Override
	public String toString() {
		return "Spout id=" + id + ", latency=" + latency + ", transfered=" + transfered + ", emitted=" + emitted
				+ ", executor=" + executor + "]";
	}

	public void setEmitted(Long emitted) {
		this.emitted = emitted;
	}

	public String getLatency() {
		return latency;
	}

	public void setLatency(String latency) {
		this.latency = latency;
	}

	public Long getTransfered() {
		return transfered;
	}

	public void setTransfered(Long transfered) {
		this.transfered = transfered;
	}
	
	
}
