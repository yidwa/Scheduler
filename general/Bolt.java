package general;

public class Bolt {
	String id;
	//String capacity;
	String executedelay;
	String processdelay;
	Long emit;
	Long transferred;


	int executor;
	Long executed;
	
	public Bolt(String id){
		this.id = id;
	//	this.capacity = "";
		this.executedelay = "";
		this.processdelay = "";
		this.emit = (long)0;
		this.executor = 0;
		this.executed = (long) 0;
		this.transferred = (long) 0;
	}
	
	@Override
	public String toString() {
		return "Bolt id=" + id + ", executedelay=" + executedelay + ", processdelay=" + processdelay + ", emit=" + emit
				+ ", transferred=" + transferred + ", executor=" + executor + ", executed=" + executed + "]";
	}

	public Long getTransferred() {
		return transferred;
	}

	public void setTransferred(Long transferred) {
		this.transferred = transferred;
	}
	public int getExecutor() {
		return executor;
	}

	public void setExecutor(int executor) {
		this.executor = executor;
	}

	public Long getExecuted() {
		return executed;
	}

	public void setExecuted(Long executed) {
		this.executed = executed;
	}

	public Long getEmit() {
		return emit;
	}

	public void setEmit(Long emit) {
		this.emit = emit;
	}

//	public String getCapacity() {
//		return capacity;
//	}

//	public void setCapacity(String capacity) {
//		this.capacity = capacity;
//	}

	public String getExecutedelay() {
		return executedelay;
	}

	public void setExecutedelay(String executedelay) {
		this.executedelay = executedelay;
	}

	public String getProcessdelay() {
		return processdelay;
	}

	public void setProcessdelay(String processdelay) {
		this.processdelay = processdelay;
	}
	
	
}
