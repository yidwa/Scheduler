package storm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.EvenScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;


import storm.model_based_scheduler.TopologyScheduler;


public class QoS_Priority{

	//schedule scheme for each topology
	public HashMap<String, TopologyScheduler> udpate = new HashMap<String, TopologyScheduler>();
	// maintain the list of schedule for each queue, like 1, [s1,m1]
	public HashMap<String, ArrayList<String>> list = new HashMap<>();
	static HashMap<String, Boolean> queueupdate = new HashMap<>();
	static HashMap<String, Integer> queueusize = new HashMap<>();
	public void prepare(Map conf) {}

	public void schedule(Topologies topologies, Cluster cluster) {
		boolean updateschedule;
		System.out.println("QoS Prioirty scheduling");	
		// collect the supervisor information
		Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
		// update the routing info
		updateschedule = feedingUpdate("/home/ubuntu/schedule");

		for(int i = 1 ; i<=3 ; i++){
			String p = String.valueOf(i);
			if(!queueupdate.containsKey(p))
				queueupdate.put(p, false);
		}

		if(updateschedule)
		{
			Collection<TopologyDetails> td;
			td = topologies.getTopologies();
	
//			List<WorkerSlot> w = new ArrayList<WorkerSlot>();
			for(TopologyDetails topology :td){
				if (topology != null) {
					boolean needsScheduling = cluster.needsScheduling(topology);
					//	    			System.out.println("topology "+topology.getName()+" , needsscheduling "+needsScheduling);
					String p = topology.getName().split("_")[1];
					boolean schedulechange = queueupdate.get(p);
					//	    			if (!needsScheduling ) 	
					if(schedulechange || needsScheduling) {
						System.out.println(topology.getName()+" needs scheduling.");
						System.out.println("start scheduling for "+topology.getName());
						findSupervisorT(cluster, topology, supervisors);
					}
					else{
						System.out.println(topology.getName()+ "does not need scheduling "); 
					}
				}
			}
			//        		new EvenScheduler().schedule(topologies, cluster);
		}
		else{
			System.out.println("the scheduling won't changed");
			Map<String, TopologyDetails> _topologies= new HashMap<String, TopologyDetails>();;
			for(TopologyDetails topology : topologies.getTopologies()){
				if(cluster.getAssignmentById(topology.getId()) == null){
					_topologies.put(topology.getId(), topology);
				}
			}
			new EvenScheduler().schedule(new Topologies(_topologies), cluster);
			System.out.print("assign topology ");
			for(String s: _topologies.keySet()){
				System.out.print(s+" , ");
			}
			System.out.println("with even scheduler");
		}
	}

	public void scheduleInstruction(){
		
	}

	  
	// find the specific supervisor according to topology
	public WorkerSlot findSupervisorT(Cluster cluster, TopologyDetails topology, Collection<SupervisorDetails> supervisors){

		WorkerSlot w = null;
		//the queue that the priority belongs 
		String p = topology.getName().split("_")[1];
		HashMap<String, ArrayList<String>> map = getList();
		ArrayList<String> assignedhost = map.get(p);
		
		if(assignedhost.size()==0){
	    		assignedhost.add("l"+p);
	    }
		    	
		//all executors belong to the topology
		List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
		Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap<>();
		
		for(String sc: topology.getComponents().keySet()){
			componentToExecutors.put(sc, topology.getComponents().get(sc).execs);
		}
		
		for(String s: componentToExecutors.keySet()){
			executors.addAll(componentToExecutors.get(s));
		}

		// the list of all nodes for the given queue
		ArrayList<SupervisorDetails> supernodes = new ArrayList<SupervisorDetails>();
//		Set<WorkerSlot> temp = new HashSet<>();
		Map<ExecutorDetails, WorkerSlot> assigned = new HashMap<ExecutorDetails, WorkerSlot>();
		
		SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
	
		if(currentAssignment!=null){
			assigned = currentAssignment.getExecutorToSlot();
		}
		
		//find all supervisor that corresponds to the priority queue
		for (SupervisorDetails supervisor : supervisors) {
			if(assignedhost.contains(supervisor.getHost())){
				supernodes.add(supervisor);
				System.out.println("adding node "+ supervisor.getHost()+" , to "+topology.getName());
			}
		}
		ArrayList<WorkerSlot> availableSlots = new ArrayList<>();
		ArrayList<WorkerSlot> assignedSlots = new ArrayList<>();
		if (supernodes != null) {
			for(ExecutorDetails ed : assigned.keySet()){
				assignedSlots.add(assigned.get(ed));
			}
		
			ArrayList<SupervisorDetails> updatelist = new ArrayList<>();	
			if(supernodes.size()>1){
				
				int[] avail = new int[supernodes.size()];
				int minind = 0;
				int maxind = supernodes.size()-1;
				for(int i = 0 ; i<supernodes.size(); i++){
					avail[i] = numofSlots(supernodes.get(i), cluster);
					//  							System.out.println("index "+i+", with number of slot "+avail[i]);
					if(avail[i]<avail[minind])
						minind = i;
					if(avail[i]>=avail[maxind])
						maxind = i;
				}
				if(avail.length==3){
					updatelist.add(supernodes.get(maxind));
					for(int j = 0; j<3; j++){
						if(j!=maxind && j!= minind)
							updatelist.add(supernodes.get(j));
					}
					updatelist.add(supernodes.get(minind));
				}
				else{
					//  							System.out.println("minind "+minind+ " , maxind "+maxind);
					updatelist.add(supernodes.get(maxind));
					updatelist.add(supernodes.get(minind));
				}		
				availableSlots = updateSlots(updatelist, cluster);
			}

			else{	
				availableSlots = updateSlots(supernodes, cluster);
			}
			
			// if there is no available slots on this supervisor, free some.
			// TODO for simplicity, we free all the used slots on the supervisor.
			if (availableSlots.isEmpty() ) {
				System.out.println("no available slots");
			}
			else{
				int index = 0;
//				System.out.println("size is "+executors.size());
				int remain = executors.size()%4;
				int templength = executors.size()/4;
				cluster.freeSlots(assignedSlots);
				for(int i =0; i<4; i++){
//					w = updateSlots(supernodes, cluster).get(0);
					if(updatelist.size() == 0)
						availableSlots = updateSlots(supernodes, cluster);
					else
						availableSlots = updateSlots(updatelist, cluster);
					
					w = availableSlots.get(0);
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
					if(updatelist.size() == 0)
						availableSlots = updateSlots(supernodes, cluster);
					else
						availableSlots = updateSlots(updatelist, cluster);
					w = availableSlots.get(0);
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

	// return the number of avail
	public int numofSlots(SupervisorDetails node, Cluster cluster){
		ArrayList<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
		for(WorkerSlot ws : cluster.getAvailableSlots(node)){
			availableSlots.add(ws);
		}
		return availableSlots.size();
	}

	

	/**
	 * Reading the host for each queue from the file
	 * @param filename
	 * @return
	 */
	public boolean feedingUpdate(String filename){
		File file = new File(filename);
		FileReader reader = null;
		BufferedReader br = null;
		String line;
//		String name = null;
		HashMap<String, ArrayList<String>> mapping = new HashMap<>();
		ArrayList<String> temp;
		boolean[] queueb = {true,true,true};

		try{
			reader = new FileReader(file);
			br = new BufferedReader(reader);
			// no info in the file
			if(br.readLine() == null){

				for(int i = 0 ; i<3 ; i++){
					ArrayList<String> s = new ArrayList<>();
					s.add("l"+(i+1));
					mapping.put(String.valueOf(i+1), s);
					QoS_Priority.queueupdate.put(String.valueOf(i+1), true);
					QoS_Priority.queueusize.put(String.valueOf(i+1), 0);
				}
			}
			else{
				while((line=br.readLine())!=null){
					//the file format should be 1 [s1,m1]
					String pri = line.substring(0, 1);
					String size = line.substring(2,3);
					String update ="false";
					int indexupdate = line.indexOf('(');
					int indexupdateright = line.indexOf(')');
					if(indexupdateright>indexupdate)
						update = line.substring(indexupdate+1,indexupdateright);
					QoS_Priority.queueupdate.put(pri, Boolean.valueOf(update));
					QoS_Priority.queueusize.put(pri, Integer.valueOf(size));
					queueb[Integer.valueOf(pri)-1] = Boolean.valueOf(update);
					int indl = line.indexOf('[');
					int indr = line.indexOf(']');
					if(indr>indl){
						String[] t = line.substring(indl+1, indr).split(",");
						temp = new ArrayList<String>();
						for(String s: t){
							temp.add(s);
						}
						mapping.put(pri, temp);
					}
				}	
			}

		}
		catch(FileNotFoundException e){
			for(int i = 0 ; i<3 ; i++){
				ArrayList<String> s = new ArrayList<>();
				s.add("l"+(i+1));
				mapping.put(String.valueOf(i+1), s);
				QoS_Priority.queueupdate.put(String.valueOf(i+1), true);
				QoS_Priority.queueusize.put(String.valueOf(i+1), 0);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		setList(mapping);
		System.out.println("inside feed upate "+queueb[0]+" , "+queueb[1] + ", "+queueb[2]);
		//		setActivenum(convertReading(temp));
		return queueb[0] || queueb[1] || queueb[2];
	}

	public HashMap<String, ArrayList<String>> getList() {
		return list;
	}

	public void setList(HashMap<String, ArrayList<String>> list) {
		this.list = list;
	}


}