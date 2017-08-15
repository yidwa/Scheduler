package storm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.set.SynchronizedSortedSet;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.EvenScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;


public class model_based_scheduler{

	//schedule scheme for each topology
	public HashMap<String, TopologyScheduler> udpate = new HashMap<String, TopologyScheduler>();
	
	public void prepare(Map conf) {}

    public void schedule(Topologies topologies, Cluster cluster) {
    	boolean reschedule;
    	System.out.println("model-based scheduling");	
    	// collect the supervisor information
    	Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
    	reschedule = feedingUpdate("/home/ubuntu/schedule");
    	if(reschedule == true){
    		Collection<TopologyDetails> td;
    		td = topologies.getTopologies();
       
    		Map<String, TopologyDetails> _topologies= new HashMap<String, TopologyDetails>();;
    		List<WorkerSlot> w = new ArrayList<WorkerSlot>();
    		
    		for(TopologyDetails topology :td){
    			if (topology != null) {
    				if(getUdpate().get(topology.getName())!=null){
    					SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
    			    	if (currentAssignment != null) {
    			    			assignCT(cluster, currentAssignment, topology, supervisors,w);
    			    		}
    			    	else{
    			    		System.out.println("no assignment for "+topology.getName());
    			    		_topologies.put(topology.getId(), topology);
    			    		}
    					}
    				else{
    						System.out.println("executor is null");
    				
    				}
    				}
    			try {
					Thread.sleep(30*1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    	}
    	if(_topologies.size() !=0){
    		System.out.println("even scheudler for "+_topologies.values().toString());
    		for(TopologyDetails tdd : _topologies.values()){
    			w.add(findSupervisorT(cluster, tdd, supervisors));
    		}
    		new EvenScheduler().schedule(new Topologies(_topologies), cluster);
    		
    		}
    	}
    	new EvenScheduler().schedule(topologies, cluster);
    }

    
    
    // get rid of executors that no need to schedule
    /**
     * 
     * @param cluster
     * @param assignment
     * @param schedulerupdate
     * @param map
     * @return
     */
    public  HashMap<String,ArrayList<String>> getNeedsScheduling(Cluster cluster,SchedulerAssignment assignment,
    		HashMap<String,String> schedulerupdate,HashMap<String,ArrayList<String>> map){ 
    	//current host and assigned exectutors
    	HashMap<String,ArrayList<String>> exe = new HashMap<String, ArrayList<String>>();
    	 Map<String,SupervisorDetails> supervisors = cluster.getSupervisors();
         Map<WorkerSlot, Collection<ExecutorDetails>> assign = assignment.getSlotToExecutors();
         ArrayList<String> temp = new ArrayList<String>();
         for(String supervisor: supervisors.keySet()){
        	 exe.put(supervisors.get(supervisor).getHost(), temp);
         }
        
         for(WorkerSlot ws: assign.keySet()){
        	 ArrayList<String> t;
        	 String host = ws.getNodeId();
        	 if(supervisors.containsKey(host)){
        		 String id  = supervisors.get(host).getHost();
        	 
        		 if(exe.get(id).size()>0){
        			 t = exe.get(id);
        		 }
        		 else{
        			 t = new ArrayList<String>();
        		 }
        		 for(ExecutorDetails ed: assign.get(ws)){
//        		 System.out.println(" , "+ed.toString());
        			 t.add(ed.toString());
        		 }
//        	 System.out.println("updaet assiment for "+id+ " with "+t.toString());
        		 exe.put(id, t);
        	 }
        	 else{
        		 System.out.println("supervisors not include this "+host);
        	 }
         
         }

        ArrayList<String> re =new ArrayList<String>();
        ArrayList<String> templist;
       	for(String s: map.keySet()){
        	 re = map.get(s);
        	 if((exe.containsKey(s))&&(re.size()>0)){
        			 for(String ss: re){
//        				 System.out.println("find exepected assignment for "+s+" , "+ss);
//        				 System.out.println("current assignment for "+s+" , "+exe.get(s).toString());
        				 if (exe.get(s).size()>0 && exe.get(s).contains(ss)){
        					 templist = map.get(s);
        					 templist.remove(ss);
        					 System.out.println(" no need to rescheduler "+ss);
        					 map.put(s,templist);
        				 }
        				 templist = new ArrayList<String>();
        			 }
        		 }
        	else{
        			 System.out.println(s+ " is not existing in the current cluster ,won't schedule for these executor");
        			 System.out.println(re.toString());
        			 map.remove(s);
        		}
       	 }

    	return map;
    }
    
    // find the specific supervisor according to topology
    public WorkerSlot findSupervisorT(Cluster cluster, TopologyDetails topology, Collection<SupervisorDetails> supervisors){
    	// find out all the needs-scheduling components of this topology
    	WorkerSlot w = null;
    	List<ExecutorDetails> componentToExecutors = null;
    	for(String sc: topology.getComponents().keySet()){
//    		System.out.println("component "+sc);
    		if(sc.equals("Spouting")){
    			componentToExecutors = topology.getComponents().get(sc).execs;
    		}
    	}
      SupervisorDetails spoutSupervisor = null;
//      String sname = t_supervisor(topology.getName());
//      System.out.println("find executors of spout "+componentToExecutors.toString());
      //assign all spouts to l2
  		for (SupervisorDetails supervisor : supervisors) {
  			if(supervisor.getHost().contains("l2")){
  				spoutSupervisor = supervisor;
  				}
  			}
  		if (spoutSupervisor != null) {
  					List<WorkerSlot> availableSlots = cluster.getAvailableSlots(spoutSupervisor);
               // if there is no available slots on this supervisor, free some.
               // TODO for simplicity, we free all the used slots on the supervisor.
  					if (availableSlots.isEmpty() ) {
  						System.out.println("no available slots in spout supervisor");
               			}
  					else{
  						w = availableSlots.get(0);
//  						System.out.println("find supervisor host ");
  						cluster.assign(w, topology.getId(), componentToExecutors);
  					}
  				}
  			
  				else{
  						System.out.println("cannot find the general supervisor");
  					}
  		return w;
    }
    
    
    
   /**
    * 
    * @param cluster
    * @param currentAssignment
    * @param topology
    * @param supervisors
    * @param ww
    */
    
    // assign executor to slot
    public void assignCT(Cluster cluster, SchedulerAssignment currentAssignment,TopologyDetails topology, 
    		Collection<SupervisorDetails> supervisors, List<WorkerSlot> ww){
//    	 find out all the needs-scheduling components of this topology
       
    	TopologyScheduler ts = getUdpate().get(topology.getName());
    	
    	HashMap<String,String> schedulerupdate = ts.getSchedulerupdate();
    	
    	// the organized list of scheduling in the format of host + executors
    	HashMap<String,ArrayList<String>> mapping = mappingEH(schedulerupdate);
    	
    	//executors except those with spout
    	Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap<String, List<ExecutorDetails>>();
    	// the executors of spout
    	List<ExecutorDetails> SExecutors = new ArrayList<ExecutorDetails>();
    	for(String sc: topology.getComponents().keySet()){
    		
// !   		componentToExecutors.put(sc,topology.getComponents().get(sc).execs);
    		if(sc != "Spouting"){
    			componentToExecutors.put(sc, topology.getComponents().get(sc).execs);
    			}
    		else
    			SExecutors.addAll(topology.getComponents().get(sc).execs);
    		}
    	mapping = getNeedsScheduling(cluster, currentAssignment, schedulerupdate, mapping);
    	
    	Set<WorkerSlot> ws = currentAssignment.getSlots();
    	
    	
    	for(WorkerSlot wws : ww){
    		if(ws.contains(wws))
    			ws.remove(wws);
    	}
    																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																		
    	if(mapping.isEmpty() || componentToExecutors.size() ==0)
    		System.out.println("no operator need udpate, stay unchanged");
    	else{
    		HashMap<String,List<ExecutorDetails>> assigning = mappingSupervisor(mapping,componentToExecutors);
    	
    		cluster.freeSlots(ws);
    		for(SupervisorDetails sd : supervisors){
    			  List<WorkerSlot> workerSlots = cluster.getAvailableSlots(sd);
    			  if (workerSlots.isEmpty() ) {
  					System.out.println("no available slots in spout supervisor");
    			  }
    			  else{
    				String host = sd.getHost();
    				if(assigning.containsKey(host) && assigning.get(host).size()>0){
    					
    					cluster.assign(workerSlots.get(0), topology.getId(), assigning.get(host));
//  						System.out.println("assign "+ mapping.get(host).toString() +" , to "+ host);
    				}
//    				else{
////    				 	System.out.println("no executors aimed to "+host);
//    				if(host.equals("l2")){
//    					workerSlots = cluster.getAvailableSlots(sd);
//    					cluster.assign(workerSlots.get(0), topology.getId(), SExecutors);
//    				}
//    				}
  				}
			}
    		List<WorkerSlot> workerSlots = cluster.getAvailableSlots();
    		List<ExecutorDetails> temp = new ArrayList<ExecutorDetails>();
    		if(cluster.getNeedsSchedulingComponentToExecutors(topology).size()>0){
    			for(String s: cluster.getNeedsSchedulingComponentToExecutors(topology).keySet()){
    				temp.addAll(cluster.getNeedsSchedulingComponentToExecutors(topology).get(s));
    				// cluster.assign(workerSlots.get(0), topology.getId(), temp);
    			}
    			//assign all rest executers to the first available slot
    			cluster.assign(workerSlots.get(0), topology.getId(), temp);
    		}
    			
    	
    	
    	}
    }
    
    
    /**
     *  the proposed scheduling scheme in the format of host + executors
     * @param map
     * @param executors
     * @return
     */
    	public HashMap<String,List<ExecutorDetails>> mappingSupervisor(HashMap<String, ArrayList<String>> map, Map<String, List<ExecutorDetails>> executors){

    		HashMap<String,List<ExecutorDetails>> result = new HashMap<>();
    		List<ExecutorDetails> executor = new ArrayList<ExecutorDetails>();
    		String r;
    		for(String host: map.keySet()){
    			result.put(host, executor);
    		}
    		for(String host: result.keySet()){
    			r ="";
    			executor = new ArrayList<ExecutorDetails>();
    			for(String s: map.get(host)){
    				r = findCname(s, executors);
    				if( r == ""){
    				}
    				else{
//    					
    					for(ExecutorDetails dd : executors.get(r)){
    						if(dd.toString().contains(s.substring(1,s.indexOf("-")))){
//    							System.out.println("find exe "+dd.toString()+ " s,  "+s+" , aims is "+r);
    	    					executor.add(dd);
    	    					
    						}
    					}
    				}
    			}
//    			System.out.println("host is "+host+" exeutor is "+executor.toString());
    			result.put(host, executor);
    		}
    		return result;
    	}
            
    	
    	/**
    	 * 
    	 * @param exeid
    	 * @param executors
    	 * @return the component name 
    	 */
    	public String findCname(String exeid,Map<String, List<ExecutorDetails>> executors){
    		exeid = exeid.trim();
    		String[] temp = exeid.split("\\[|\\]|-");
//    		for(String st : temp)
//    			System.out.print(st +",");
    		exeid = "["+temp[1]+", "+temp[1]+"]";
//    		System.out.println("find Cname:"+exeid);
    		String cid = "";
//    		System.out.println("FINDCNAME "+exeid);
    		for(String s: executors.keySet()){
//    			System.out.println("show "+s+" , executor "+ executors.get(s).toString());
    			for(ExecutorDetails ed : executors.get(s)){
    				if(ed.toString().equals(exeid)){
    					cid = s;
        				return cid;
    				}
    			}
    			
    		}
    		return cid;
    	}
//    	public String findCname(String exeid){
//    		String s = "";
////    		if(exeid.contains("[1-1]")){
////    			s = "TSpouting";
////    		}
////    		else if(exeid.contains("[2-2]")){
////    			s = "FSpouting";
////    		}
//    	    if(exeid.contains("[11-11]")){
//    			s = "Tappending_a";
//    		}
//    		else if(exeid.contains("[12-12]")){
//    			s = "Fappending_a";
//    		}
//    		else if(exeid.contains("[13-13]")){
//    			s = "Tappending_b";
//    		}
//    		else if(exeid.contains("[14-14]")){
//    			s = "Fappending_b";
//    		}
//    		else if(exeid.contains("[15-15]")){
//    			s = "Tappending_c";
//    		}
//    		else if(exeid.contains("[16-16]")){
//    			s = "Fappending_c";
//    		}
//    		else if(exeid.contains("[17-17]")){
//    			s = "Tappending_d";
//    		}
//    		else if(exeid.contains("[18-18]")){
//    			s = "Fappending_d";
//    		}
//    		else if(exeid.contains("[19-19]")){
//    			s = "Tremovelast";
//    		}
//    		else if(exeid.contains("[20-20]")){
//    			s = "Fremovelast";
//    		}
//    		return s;
//    	}
//    			System.out.println(id+" is supposed to assign to "+schedulerupdate.get(id));
//    		}
    	
    
    /**
     * get the proposed scheduling scheme in the format of host + executors
     * @param schdraft
     * @return for each host, giving the proposed executor id
     */
    
    	public HashMap<String,ArrayList<String>> mappingEH(HashMap<String,String> schdraft){
    		HashMap<String,ArrayList<String>> result = new HashMap<>();
    		ArrayList<String> temp;
    		// for each executor in the schedule scheme
    		for(String s:schdraft.keySet()){
    			String type = schdraft.get(s);
    			if(result.containsKey(type)){
    				temp = result.get(type);
    			}
    			else{
    				temp = new ArrayList<>();
    			}
    			temp.add(s);
    			result.put(type, temp);
    		}
    		return result;
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
//		model_based_scheduler mbs = new model_based_scheduler();
//		mbs.feedingUpdate("schedule");
//		for(String s: mbs.getUdpate().keySet()){
//			System.out.println(s+" , "+mbs.getUdpate().get(s).schedulerupdate.toString());
//			}
//		}
//    
//    
    /**
     * Reading information from the optimized schedule
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
		try{
			reader = new FileReader(file);
			br = new BufferedReader(reader);
			TopologyScheduler ts = null;
			String[] info;
			if(br.readLine() == null)
				reschedule = false;
			while((line=br.readLine())!=null){
				if(line.contains("star")||line.contains("line")||line.contains("diamond")){
					name = line;
					ts = new TopologyScheduler(name);
				}
				else{
					if(line.length()>1){
					line = line.substring(1, line.length()-1);
					info = line.split(",");
					for(String s: info){
						String[] temp = s.split("=");
						ts.getSchedulerupdate().put(temp[0], temp[1]);
					}
					getUdpate().put(name, ts);
					}
				}
			}
//			file.renameTo(new File("/home/ubuntu/schedulebackup");
		}
		catch(FileNotFoundException e){
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return reschedule;
	}



	public HashMap<String, TopologyScheduler> getUdpate() {
		return udpate;
	}



	class TopologyScheduler{
		HashMap<String,String> schedulerupdate;
		String tid;
		TopologyScheduler(String tid){
			this.tid = tid;
			this.schedulerupdate = new HashMap<String, String>();
		}
		public HashMap<String, String> getSchedulerupdate() {
			return schedulerupdate;
		}
		public void setSchedulerupdate(HashMap<String, String> schedulerupdate) {
			this.schedulerupdate = schedulerupdate;
		}
		
	
	}
}