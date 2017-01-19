package general;

public class WorkerProcess {
	String tid;
	Executor exe;
	public WorkerProcess(String tid, Executor exe){
		this.tid = tid;
		this.exe = exe;
	}
}
