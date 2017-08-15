package storm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.EvenScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;

import storm.model_based_scheduler.TopologyScheduler;


public class QoS_scheduler{

	//schedule scheme for each topology
	public HashMap<String, TopologyScheduler> udpate = new HashMap<String, TopologyScheduler>();
	// the active number of host for each queue q1,q2,q3 for type large, medium and small
	public int[] activenum = {0,0,0};
	public void prepare(Map conf) {}

    public void schedule(Topologies topologies, Cluster cluster) {
    	boolean reschedule;
    	System.out.println("QoS scheduling");	
    	// collect the supervisor information
    	Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
    	reschedule = feedingUpdate("/home/ubuntu/schedule");
//    	reschedule = feedingUpdate("/Users/yidwa/Desktop/schedule");
    	if(reschedule == true){
    		Collection<TopologyDetails> td;
    		td = topologies.getTopologies();
       
//    		Map<String, TopologyDetails> _topologies= new HashMap<String, TopologyDetails>();;
    		List<WorkerSlot> w = new ArrayList<WorkerSlot>();
    		for(TopologyDetails topology :td){
	    		if (topology != null) {
	    			boolean needsScheduling = cluster.needsScheduling(topology);
	    			if (!needsScheduling) {
	    				System.out.println(topology.getName() + " DOES NOT NEED scheduling.");
	    			} 	
	    			else {
	    				System.out.println(topology.getName()+" needs scheduling.");
	    				System.out.println("start scheduling for "+topology.getName());
	    				findSupervisorT(cluster, topology, supervisors);
	    			}
	    		}
	    	}
//        		new EvenScheduler().schedule(topologies, cluster);
    	}
    }

  
     

    
    // find the specific supervisor according to topology
    public WorkerSlot findSupervisorT(Cluster cluster, TopologyDetails topology, Collection<SupervisorDetails> supervisors){
    	// find out all the needs-scheduling components of this topology
    	WorkerSlot w = null;
    	//the queue that the priority belongs 
    	String nodeclass = "";
    	//the priority obtained from the topology name
//    	int priority = topology.getTopologyPriority();
    	String temp = topology.getName().split("_")[1];
    	int priority = Integer.valueOf(temp);
    	List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
    	
        Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
        for(String s: componentToExecutors.keySet()){
			executors.addAll(componentToExecutors.get(s));
    
//    		componentToExecutors = topology.getComponents().get(sc).execs;
    	}
    
    	// the list of all nodes for the given queue
    	ArrayList<SupervisorDetails> supernodes = new ArrayList<SupervisorDetails>();
    	
    	//find all supervisor that corresponds to the priority queue
  		for (SupervisorDetails supervisor : supervisors) {
  		
  			System.out.println("topology name is "+ topology.getName()+"priority is "+priority);
  			if(priority == 1 && supervisor.getHost().contains("l")){
  				supernodes.add(supervisor);
  				System.out.println("adding "+supervisor.getHost()+" to nodes");
  				}
  			else if(priority == 2 && supervisor.getHost().contains("m")){
  				supernodes.add(supervisor);
  				System.out.println("adding "+supervisor.getHost()+" to nodes");
  			}
  			else if(priority == 3 && supervisor.getHost().contains("s")){
  				supernodes.add(supervisor);
  				System.out.println("adding "+supervisor.getHost()+" to nodes");
  				}
  			}
  		if (supernodes != null) {
  					ArrayList<WorkerSlot> availableSlots = updateSlots(supernodes, cluster);
//  					   ArrayList<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
//  					for(SupervisorDetails sd : supernodes){
////  						availableSlots.addAll(cluster.getAvailableSlots(sd));
//  							for(WorkerSlot ws : cluster.getAvailableSlots(sd)){
//  								availableSlots.add(ws);
//  							}
//  					}
  					
               // if there is no available slots on this supervisor, free some.
               // TODO for simplicity, we free all the used slots on the supervisor.
  					if (availableSlots.isEmpty() ) {
  						System.out.println("no available slots in spout supervisor");
               			}
  					else{
  						System.out.println("size is "+executors.size());
  						int remain = executors.size()%4;
  						int templength = executors.size()/4;
  						System.out.println("temp length is "+templength+" ,and remain is "+remain);
  						int index = 0;	
  						for(int i =0; i<4; i++){
  							w = updateSlots(supernodes, cluster).get(0);
  							ArrayList<ExecutorDetails> part = new ArrayList<ExecutorDetails>();
  							for(int j = index; j<index+templength; j++){
  								part.add(executors.get(j));
  							}
  							index = index+templength;
  							System.out.println("executors are "+part.toString());
  							cluster.assign(w, topology.getId(), part);
  							System.out.println("assign topology "+topology.getName()+ " , to "+w.getNodeId()+" , "+w.getPort());
  						}
  						if(remain >0){
  							w = updateSlots(supernodes, cluster).get(0);
  							ArrayList<ExecutorDetails> part = new ArrayList<ExecutorDetails>();
  							for(int i = index; i<index+remain; i++){
  								part.add(executors.get(i));
  							}
  							System.out.println("executors are "+part.toString());
  							cluster.assign(w, topology.getId(), part);
  							System.out.println("assign topology "+topology.getName()+ " , to "+w.getNodeId()+" , "+w.getPort());
  						}
  					}
  				}
  		
  	
  		return w;
    }
    
    /**
     * update the available slot
     * @param nodes
     * @return
     */
    public ArrayList<WorkerSlot> updateSlots(ArrayList<SupervisorDetails> nodes, Cluster cluster){
    	 ArrayList<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
			for(SupervisorDetails sd : nodes){
//				availableSlots.addAll(cluster.getAvailableSlots(sd));
					for(WorkerSlot ws : cluster.getAvailableSlots(sd)){
						availableSlots.add(ws);
					}
			}
		return availableSlots;
    }
 
    //mapping rationship between topology name and supervisor for testing purpose
//    public String t_supervisor(String tname){
//    	if(tname.contains("line"))
//    			return "large";
//    	else if(tname.contains("star"))
//    			return "medium";
//    	else if(tname.contains("diamond"))
//    		return "large";
//    	else 
//    		return "general";
//    				
//    		
//    }
  
    
//    public static void main(String[] args) {
//    	QoS_scheduler qos = new QoS_scheduler();
//    	qos.feedingUpdate("/Users/yidwa/Desktop/schedule");
//    	for(int i : qos.activenum){
//    		System.out.println(i);
//    		}
//    	}
//    
    
    /**
     * Reading the NO.active host for each queue from the file
     * @param filename
     * @return
     */
	public boolean feedingUpdate(String filename){
		boolean reschedule = true;
		File file = new File(filename);
		FileReader reader = null;
		BufferedReader br = null;
		String line;
		String name = null;
		String[] temp = {"","",""};
		try{
			reader = new FileReader(file);
			br = new BufferedReader(reader);
//			TopologyScheduler ts = null;
			if(br.readLine() == null)
				reschedule = false;
			while((line=br.readLine())!=null){
				//the file format should be large 1 for the first line
				if(line.contains("large")){
					temp[0] = line.split(" ")[1];
				}
				else if(line.contains("medium")){
					temp[1] = line.split(" ")[1];
					}
				else{
					temp[2] = line.split(" ")[1];
				}
			}
		}
		catch(FileNotFoundException e){
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		setActivenum(convertReading(temp));
		return reschedule;
	}
	
	public int[] convertReading(String[] temp){
		int[] result = new int[temp.length];
		for(int i = 0 ; i<temp.length; i++){
			result[i] = Integer.valueOf(temp[i]);
		}
		return result;
	}

	public int[] getActivenum() {
		return activenum;
	}

	public void setActivenum(int[] activenum) {
		this.activenum = activenum;
	}
	
}