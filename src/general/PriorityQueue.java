package general;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import model.QueueLatency;

// the queues with host with different size
public class PriorityQueue {
	int prioirty;
	int size;
	ArrayList<String> names;
	
	LinkedList<Double> arr;
	LinkedList<Double> serv;
	ArrayList<String> hosts;
	QueueLatency ql;
	Double waittime;
	HashMap<String,Double> buffertime;
	double avgbuf;

	public PriorityQueue(int pri, int size, ArrayList<String> names){
		this.prioirty = pri;
		this.size = size;
		this.names = names;
		this.arr = new LinkedList<>();
		this.serv = new LinkedList<>();
		this.hosts = new ArrayList<String>();
		this.ql = new QueueLatency(serv, arr, hosts.size(), pri);
		this.waittime = 0.0;
		this.buffertime = new HashMap<String,Double>();
		this.avgbuf = 0.0;
	}
	
	public ArrayList<String> getHosts() {
		return hosts;
	}

	public void setHosts(ArrayList<String> hosts) {
		this.hosts = hosts;
	}

	public int getPrioirty() {
		return prioirty;
	}
	public void setPrioirty(int prioirty) {
		this.prioirty = prioirty;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public ArrayList<String> getNames() {
		return names;
	}
	public void setNames(ArrayList<String> names) {
		this.names = names;
	}
	public LinkedList<Double> getArr() {
		return arr;
	}

	// need to add set for the latency ~~~~~
	public void setArr(LinkedList<Double> arr) {
		this.arr = arr;
	}

	public LinkedList<Double> getServ() {
		return serv;
	}

	public void setServ(LinkedList<Double> serv) {
		this.serv = serv;
	}

	public Double getWaittime() {
		return waittime;
	}

	public void setWaittime(Double waittime) {
		this.waittime = waittime;
	}

	public QueueLatency getQl() {
		return ql;
	}

	public void setQl(QueueLatency ql) {
		this.ql = ql;
	}

	public HashMap<String, Double> getBuffertime() {
		return buffertime;
	}

	public void setBuffertime(HashMap<String, Double> buffertime) {
		this.buffertime = buffertime;
	}

	public double getAvgbuf() {
		return avgbuf;
	}

	public void setAvgbuf(double avgbuf) {
		this.avgbuf = avgbuf;
	}
	
}
