package general;

import java.util.ArrayList;
import java.util.HashMap;

public class StormCluster {
	public HashMap<String, Topology> topologies ;
	public HashMap<String, Integer> priority;
	public StormREST sr;
	public ArrayList<PriorityQueue> queue;
	
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
		sr.Topologyget(topologies, priority, true,queue);
		sr.Topologyinfo(this.topologies);
		
	}
	
}
