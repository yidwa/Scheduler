package storm;




import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.set.SynchronizedSortedSet;
import org.apache.storm.config__init;
//import org.apache.storm.config.override_login_config_with_system_property;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.EvenScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;


public class spouting_scheduler {


	    public void prepare(Map conf) {}

	    public void schedule(Topologies topologies, Cluster cluster) {
	    	System.out.println("topology re-construct scheduling");
	        // Gets the topology which we want to schedule
	    	
	       // TopologyDetails topology = topologies.getByName("line");
	    	Collection<TopologyDetails> td;
	    	td = topologies.getTopologies();
	        Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
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
	    	getUsage(supervisors);
	        new EvenScheduler().schedule(topologies, cluster);
	    			
	    		
	    }
	    // find the specific supervisor according to topology
	    public void findSupervisorT(Cluster cluster, TopologyDetails topology, Collection<SupervisorDetails> supervisors){
	    	// find out all the needs-scheduling components of this topology
          Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
          SupervisorDetails spoutSupervisor = null;
          SupervisorDetails boltSupervisor = null;
          String sname = t_supervisor(topology.getName());
          
      	for (SupervisorDetails supervisor : supervisors) {
              Map meta = (Map) supervisor.getSchedulerMeta();
              if (meta != null && meta.get("name") != null){
              	System.out.println("meta "+meta.get("name"));
              	if (meta.get("name").equals("general")) {
              		spoutSupervisor = supervisor;
              		System.out.println("find general supervisor");
              	}
//              	else if( meta.get("name").equals(sname)){
//               	    boltSupervisor = supervisor;
//               	    System.out.println("find bolt supervisor");
//               }
              }
      	}
//      	if(componentToExecutors.containsKey("Spouting")){
      	for(String s: componentToExecutors.keySet()){
      			List<ExecutorDetails> executors = componentToExecutors.get(s);
      		
//      		String sname = "";
//      		if(s.equals("Spouting"))
//      			sname ="general";
//      			sname = "general";
//      		else
//      		    sname = t_supervisor(topology.getName());
      			System.out.println("assign for "+s);
//      		
      			if(s.equals("Spouting")){
      				
               // found the special supervisor
      				if (spoutSupervisor != null) {
                  
      					List<WorkerSlot> availableSlots = cluster.getAvailableSlots(spoutSupervisor);
      					System.out.println("find spout supervisor with "+ availableSlots.size() + " slots");
                   // if there is no available slots on this supervisor, free some.
                   // TODO for simplicity, we free all the used slots on the supervisor.
      					if (availableSlots.isEmpty() && !executors.isEmpty()) {
      						System.out.println("no available slots in spout supervisor");
//                       for (Integer port : cluster.getUsedPorts(spoutSupervisor)) {
//                           cluster.freeSlot(new WorkerSlot(spoutSupervisor.getId(), port));
//                           System.out.println("new worker slot created at port "+port);
//                       		}
//                       // re-get the aviableSlots
//                       availableSlots = cluster.getAvailableSlots(spoutSupervisor);
                   			}
      					else{
      						cluster.assign(availableSlots.get(0), topology.getId(), executors);
      					}
      				}
      			
      				else{
      						System.out.println("cannot find the general supervisor");
      					}
      				}
      			
//      			else{
//      				if(boltSupervisor != null){
////      					boltSupervisor.g
//      					List<WorkerSlot> availableSlots = cluster.getAvailableSlots(boltSupervisor);
//      					System.out.println("find bolt supervisor with "+ availableSlots.size() + " slots");
//      					if (availableSlots.isEmpty() && !executors.isEmpty()) {
//      							System.out.println("no avialble slot in bolt supervisor");
//      						}
////      							for (Integer port : cluster.getUsedPorts(boltSupervisor)) {
////      								cluster.freeSlot(new WorkerSlot(boltSupervisor.getId(), port));
////      								System.out.println("new worker slot created at port "+port);
////                         }
//                          // re-get the aviableSlots
//      						else
////      							availableSlots = cluster.getAvailableSlots(boltSupervisor);
//      							cluster.assign(availableSlots.get(0), topology.getId(), executors);
//      				}
//      			
//      				else{
//      					System.out.println("cannot find "+sname+" supervisor");
//      				}
//      				}
      		}
      	
      	
	    }
	    
	    public void getUsage(Collection<SupervisorDetails> supervisors){
	    	String s ="";
	    	for (SupervisorDetails supervisor : supervisors) {
	    		s+= supervisor.getHost()+ " cpu " +String.valueOf(supervisor.getTotalCPU());
	    		s+= " mem "+String.valueOf(supervisor.getTotalMemory());
	    	}
	    	try {
				String path = "/home/ubuntu/supervisor_usage";
//				String path = "/home/ubuntu/TopologyResult.txt";
				File f = new File(path);
				FileWriter fw = new FileWriter(f,true);
				String time = formattime();
				fw.write(time+" , "+ s+"\n");
				System.out.println("write "+ s);
				fw.flush();
					
				fw.close();
				}
				catch (IOException e1) {
						// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	    }
	    //mapping rationship between topology name and supervisor for testing purpose
	    public String t_supervisor(String tname){
	    	if(tname.contains("line"))
	    			return "large";
	    	else if(tname.contains("star"))
	    			return "medium";
	    	else if(tname.contains("diamond"))
	    		return "large";
	    	else 
	    		return "general";
	    				
	    		
	    }
	    public static String formattime(){
			Instant instant = Instant.now (); // Current date-time in UTC.
			String output = instant.toString ();
			output = instant.toString ().replace ( "T" , " " ).replace( "Z" , "");
			return output;
		}
	}

