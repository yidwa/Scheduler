package general;

import java.util.HashMap;

public class StormCluster {
	public HashMap<String, Topology> topologies ;
	
	public StormREST sr;
	
	public StormCluster(){
//		System.out.println("storm cluster check 1");
		this.sr = new StormREST("http://115.146.85.187:8080");
//		System.out.println("storm cluster check 2");
		topologies = new HashMap<String,Topology>();
		sr.Topologyget(topologies, true);
//		System.out.println("storm cluster check 3");
		sr.Topologyinfo(this.topologies);
	}
	
	
	
	
}
