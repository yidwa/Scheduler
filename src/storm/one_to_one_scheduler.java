//package org.apache.storm.scheduler;
package storm;

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

public class one_to_one_scheduler implements IScheduler{
	/**
	 * This demo scheduler make sure a spout named <code>special-spout</code> in topology <code>special-topology</code> runs
	 * on a supervisor named <code>special-supervisor</code>. supervisor does not have name? You can configure it through
	 * the config: <code>supervisor.scheduler.meta</code> -- actually you can put any config you like in this config item.
	 * 
	 * In our example, we need to put the following config in supervisor's <code>storm.yaml</code>:
	 * <pre>
	 *     # give our supervisor a name: "special-supervisor"
	 *     supervisor.scheduler.meta:
	 *       name: "special-supervisor"
	 * </pre>
	 * 
	 * Put the following config in <code>nimbus</code>'s <code>storm.yaml</code>:
	 * <pre>
	 *     # tell nimbus to use this custom scheduler
	 *     storm.scheduler: "storm.DemoScheduler"
	 * </pre>
	 * @author xumingmingv May 19, 2012 11:10:43 AM
	 */
	
	
	//have changed the testingworker supervisor to superone

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
	        new EvenScheduler().schedule(topologies, cluster);
	    			
	                // find out all the needs-scheduling components of this topology
////	                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
//	                
////	                System.out.println("needs scheduling(component->executor): " + componentToExecutors);
////	                System.out.println("needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
//	                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName(topology.getName()).getId());
//	                if (currentAssignment != null) {
//	                	System.out.println(topology.getName()+ " current assignments: " + currentAssignment.getExecutorToSlot());
//	                } else {
//	                	System.out.println(topology.getName()+ " current assignments: {}");
//	                }
//	                
	                
	            
	                // try to assign the bolt accroding to their topology for testing purpose  
//	                if (!componentToExecutors.containsKey("Spouting")) {
////	                	
//	                	System.out.println(" spout DOES NOT NEED scheduling.");
//	                } else {
	                
//	    		for(String s: componentToExecutors.keySet()){
//	    			  if(s == "Spouting"){
//	                   System.out.println(s+ " needs scheduling.");
////	                    List<ExecutorDetails> executors = componentToExecutors.get("Spouting");
//	                   List<ExecutorDetails> executors = componentToExecutors.get(s);
//	                    // find out the our "special-supervisor" from the supervisor metadata
//	                 
////	                    System.out.println("supervisors size "+supervisors.size());
//	                    for (SupervisorDetails supervisor : supervisors) {
//	                    	System.out.println("supervisor " + supervisor.getId()+ " , "+supervisor.getMeta());
//	                        Map meta = (Map) supervisor.getSchedulerMeta();
//	                        if (meta != null && meta.get("name") != null && meta.get("name").equals("general")) {
//	                            System.out.println("find the general supervisor "+supervisor.toString());
//	                        	specialSupervisor = supervisor;
//	                            break;
//	                        }
//	                    }
//	                    // found the special supervisor
//	                    if (specialSupervisor != null) {
//	                    	System.out.println("Found the special-supervisor");
//	                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);
//	                       
//	                        // if there is no available slots on this supervisor, free some.
//	                        // TODO for simplicity, we free all the used slots on the supervisor.
//	                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
//	                            for (Integer port : cluster.getUsedPorts(specialSupervisor)) {
//	                                cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), port));
//	                                System.out.println("new worker slot created at port "+port);
//	                            }
//	                        }
//
//	                        // re-get the aviableSlots
//	                        availableSlots = cluster.getAvailableSlots(specialSupervisor);
////	                        WorkerSlot ws = availableSlots.get(1);
//	                        // since it is just a demo, to keep things simple, we assign all the
//	                        // executors into one slot.
//	                        cluster.assign(availableSlots.get(0), topology.getId(), executors);
//	                        System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
//	                       
//	                    } else {
//	                    	System.out.println("There is no supervisor named general");
//	                    }  
//	    			  }
//	    			}
//	                
//	            }
//	        }
	        
	        // let system's even scheduler handle the rest scheduling work
	        // you can also use your own other scheduler here, this is what
	        // makes storm's scheduler composable.
	    
	    	//remove general when only assign spout to general supervisor
//	        supervisors.remove(specialSupervisor);
	    		
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
                	else if( meta.get("name").equals(sname)){
                 	    boltSupervisor = supervisor;
                 	    System.out.println("find bolt supervisor");
                 }
                }
        	}
//        	if(componentToExecutors.containsKey("Spouting")){
        	for(String s: componentToExecutors.keySet()){
        			List<ExecutorDetails> executors = componentToExecutors.get(s);
        		
//        		String sname = "";
//        		if(s.equals("Spouting"))
//        			sname ="general";
//        			sname = "general";
//        		else
//        		    sname = t_supervisor(topology.getName());
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
//                         for (Integer port : cluster.getUsedPorts(spoutSupervisor)) {
//                             cluster.freeSlot(new WorkerSlot(spoutSupervisor.getId(), port));
//                             System.out.println("new worker slot created at port "+port);
//                         		}
//                         // re-get the aviableSlots
//                         availableSlots = cluster.getAvailableSlots(spoutSupervisor);
                     			}
        					else{
        						cluster.assign(availableSlots.get(0), topology.getId(), executors);
        					}
        				}
        			
        				else{
        						System.out.println("cannot find the general supervisor");
        					}
        				}
        			
        			else{
        				if(boltSupervisor != null){
        					
        					List<WorkerSlot> availableSlots = cluster.getAvailableSlots(boltSupervisor);
        					System.out.println("find bolt supervisor with "+ availableSlots.size() + " slots");
        					if (availableSlots.isEmpty() && !executors.isEmpty()) {
        							System.out.println("no avialble slot in bolt supervisor");
        						}
//        							for (Integer port : cluster.getUsedPorts(boltSupervisor)) {
//        								cluster.freeSlot(new WorkerSlot(boltSupervisor.getId(), port));
//        								System.out.println("new worker slot created at port "+port);
//                           }
                            // re-get the aviableSlots
        						else
//        							availableSlots = cluster.getAvailableSlots(boltSupervisor);
        							cluster.assign(availableSlots.get(0), topology.getId(), executors);
        				}
        			
        				else{
        					System.out.println("cannot find "+sname+" supervisor");
        				}
        				}
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
	  
	}

