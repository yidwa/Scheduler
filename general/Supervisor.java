package general;

import java.util.LinkedList;
import java.util.Queue;

public class Supervisor {
	String id;
//	String ip;
	long totalslot;
	long usedslot;
	double totalMem;
	double totalCPU;
	double usedMem;
	double usedCPU;
	Queue memhis;
    Queue cpuhis;
	
	public Supervisor(String id, long totalslot, long usedslot, double totalMem, double totalCPU, double usedMem, double usedCPU){
		this.id = id;
	//	this.ip = ip;
		this.totalslot = totalslot;
		this.usedslot = usedslot;
		this.totalCPU = totalCPU;
		this.usedCPU = usedCPU;
		this.totalMem = totalMem;
		this.usedMem = usedMem;
		this.memhis = new LinkedList();
		this.cpuhis = new LinkedList();
	}

	
	@Override
	public String toString() {
		return "Supervisor [id=" + id + ", totalslot=" + totalslot + ", usedslot=" + usedslot
				+ ", totalMem=" + totalMem + ", totalCPU=" + totalCPU + ", usedMem=" + usedMem + ", usedCPU=" + usedCPU
				+ "]";
	}


	public void updatecpu(double value){
		getCpuhis().add(value);
	}
	
	public void updatemem(double value){
		getMemhis().add(value);
	}
	
	
	public Queue<Double> getMemhis() {
		return memhis;
	}

	public void setMemhis(Queue memhis) {
		this.memhis = memhis;
	}

	public Queue<Double> getCpuhis() {
		return cpuhis;
	}

	public void setCpuhis(Queue<Integer> cpuhis) {
		this.cpuhis = cpuhis;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

//	public String getIp() {
//		return ip;
//	}
//
//	public void setIp(String ip) {
//		this.ip = ip;
//	}

	public long getTotalslot() {
		return totalslot;
	}

	public void setTotalslot(int totalslot) {
		this.totalslot = totalslot;
	}

	public long getUsedslot() {
		return usedslot;
	}

	public void setUsedslot(long usedslot) {
		this.usedslot = usedslot;
	}

	public double getTotalMem() {
		return totalMem;
	}

	public void setTotalMem(double totalMem) {
		this.totalMem = totalMem;
	}

	public double getTotalCPU() {
		return totalCPU;
	}

	public void setTotalCPU(double totalCPU) {
		this.totalCPU = totalCPU;
	}

	public double getUsedMem() {
		return usedMem;
	}

	public void setUsedMem(double usedMem) {
		this.usedMem = usedMem;
	}

	public double getUsedCPU() {
		return usedCPU;
	}

	public void setUsedCPU(double usedCPU) {
		this.usedCPU = usedCPU;
	}
	
	
}
