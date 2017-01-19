package general;

import java.util.ArrayList;
import java.util.HashMap;

public class Topology {
	HashMap<String, Long> tworker;
	String id;
	ArrayList<Bolt> bolts;
	Spout spout;
	
	public Topology(String id){
		this.id = id;
		this.tworker = new HashMap<String, Long>();
		this.bolts = new ArrayList<Bolt>();
	}

	public HashMap<String, Long> getTworker() {
		return tworker;
	}

	public ArrayList<Bolt> getBolts() {
		return bolts;
	}

	public void setBolts(ArrayList<Bolt> bolts) {
		this.bolts = bolts;
	}

	public Spout getSpout() {
		return spout;
	}

	public void setSpout(Spout spout) {
		this.spout = spout;
	}
	
	

}
