package general;

public class ExecutorStats {
	String tid;
	String cid;
	Executor exe;
	Long tran;
	Long emit;
	Double latency;
	
	public ExecutorStats(String tid, String cid, Executor exe){
		this.tid = tid;
		this.cid = cid;
		this.exe = exe;
	}
	
	public String getTid() {
		return tid;
	}
	public void setTid(String tid) {
		this.tid = tid;
	}
	public String getCid() {
		return cid;
	}
	public void setCid(String cid) {
		this.cid = cid;
	}
	public Executor getExe() {
		return exe;
	}
	public void setExe(Executor exe) {
		this.exe = exe;
	}
	public Long getTran() {
		return tran;
	}
	public void setTran(Long tran) {
		this.tran = tran;
	}
	public Long getEmit() {
		return emit;
	}
	public void setEmit(Long emit) {
		this.emit = emit;
	}
	public Double getLatency() {
		return latency;
	}
	public void setLatency(Double latency) {
		this.latency = latency;
	}
	
}
