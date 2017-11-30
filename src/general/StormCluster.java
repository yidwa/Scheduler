package general;

import java.util.ArrayList;
import java.util.HashMap;

public class StormCluster {
	public static HashMap<String, Topology> topologies ;
	public static HashMap<String, Integer> priority;
	public static StormREST sr;
	public static ArrayList<PriorityQueue> queue;
	
	// update the topology and topology info, create the connection
	public StormCluster(){
		this.sr = new StormREST("http://115.146.86.60:8080");
		topologies = new HashMap<String,Topology>();
		priority = new HashMap<String,Integer>();
		queue = new ArrayList<PriorityQueue>();
		PriorityQueue pq1 = new PriorityQueue(1, 0, new ArrayList<String>());
		PriorityQueue pq2 = new PriorityQueue(2, 0, new ArrayList<String>());
		PriorityQueue pq3 = new PriorityQueue(3, 0, new ArrayList<String>());
		queue.add(pq1);
		queue.add(pq2);
		queue.add(pq3);
		
//		sr.getQueue(queue);
//		sr.Topologyget(topologies, priority, true,queue);
		sr.Topologyget(true);
		sr.Topologyinfo(this.topologies);
		
	}

	public HashMap<String, Topology> getTopologies() {
		return topologies;
	}

	public void setTopologies(HashMap<String, Topology> topologies) {
		this.topologies = topologies;
	}

	public HashMap<String, Integer> getPriority() {
		return priority;
	}

	public void setPriority(HashMap<String, Integer> priority) {
		this.priority = priority;
	}

	public ArrayList<PriorityQueue> getQueue() {
		return queue;
	}

	public void setQueue(ArrayList<PriorityQueue> queue) {
		this.queue = queue;
	}
	
	
}
