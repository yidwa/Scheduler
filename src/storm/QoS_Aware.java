package storm;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.set.SynchronizedSortedSet;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;

import general.PriorityQueue;
import storm.model_based_scheduler.TopologyScheduler;

public class QoS_Aware {


	// TODO Auto-generated constructor stub
	//schedule scheme for each topology
	//		public HashMap<String, TopologyScheduler> udpate = new HashMap<String, TopologyScheduler>();
	// maintain the list of schedule for each queue, like 1, [s1,m1]
	public HashMap<String, ArrayList<String>> list = new HashMap<>();

	public static HashMap<Integer, Boolean> queueupdate = new HashMap<>();
	static HashMap<String, Integer> queueusize = new HashMap<>();
	public static HashMap<String, ArrayList<String>> workerlist = new HashMap<>();

	public void prepare(Map conf) {}

	public void schedule(Topologies topologies, Cluster cluster) {
		boolean reschedule;

		System.out.println("QoS Priority scheduling");	
		// collect the supervisor information
		Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();

		reschedule = feedingUpdate("/home/ubuntu/schedule");
		//		System.out.println("update done "+QoS_Aware.queueupdate.size()+" , "+QoS_Aware.queueusize.size());
		//	    	reschedule = feedingUpdate("/Users/yidwa/Desktop/schedule");

		// at least one of the queue need update
		Collection<TopologyDetails> td;
		td = topologies.getTopologies();
		HashMap<Integer, ArrayList<TopologyDetails>> queuelist = new HashMap<>();
		
		if(reschedule == true){
			//keep the queue that need to update
//			ArrayList<Integer> updatedQueue = new ArrayList<>();
//			for(int i : queueupdate.keySet()){
//				if(QoS_Aware.queueupdate.get(i))
//					updatedQueue.add(i);
//			}
			

			// list of topology of each queue
		

			for(int i = 1; i<=3; i++){
				queuelist.put(i, new ArrayList<>());
			}


			for(TopologyDetails topology :td){
				int p = getQueue(topology);
				String id = topology.getId();
				// the topology belongs to the updated queue
//				if (topology != null) {
					//		    			boolean needsScheduling = cluster.needsScheduling(topology);
					//		    			if (!needsScheduling && !queueup) {
					//		    				System.out.println(topology.getName() + " DOES NOT NEED scheduling.");
					//		    			} 	
					//		    			else {
				queuelist.get(p).add(topology);
				System.out.println("add "+topology.getId()+" to queue "+p);
					//		    				System.out.println(topology.getName()+" needs scheduling.");
					//		    				System.out.println("start scheduling for "+topology.getName());
					//		    				assigning(cluster, topology, supervisors);
					//		    			}
//				}
//				else{
//				if (cluster.getAssignmentById(topology.getId()) == null){
//					queuelist.get(p).add(topology);	
//					System.out.println("topology "+topology.getId()+" is initialized");
//					}
				}
				assigning(cluster, queuelist, supervisors);
			}
			//	        		new EvenScheduler().schedule(topologies, cluster);
		else{
			for(TopologyDetails topology :td){
				if (cluster.getAssignmentById(topology.getId()) == null){
					queuelist.get(getQueue(topology)).add(topology);
				}
			}
			assigning(cluster, queuelist, supervisors);
		}
	}


	/**
	 * assign based on queue
	 * @param cluster
	 * @param queuelist , the list of updated queue list
	 * @param supervisors
	 */
	public void assigning(Cluster cluster, HashMap<Integer, ArrayList<TopologyDetails>> queuelist, Collection<SupervisorDetails> supervisors){
		ArrayList<String> hostname;


		// for each queue, decide the rescheduling scheme based on size and list
		for(int i : queuelist.keySet()){
			hostname = new ArrayList<>();
			boolean changedschedule = QoS_Aware.queueupdate.get(i);
			int size = QoS_Aware.queueusize.get(String.valueOf(i));
			// if size == 0, then the schedule has changed, then need to reassign the executors to the new one
			for(String s: list.get(String.valueOf(i))){
				hostname.add(s);
			}
			// the whole queue has changed schedule
			if(changedschedule){
			// the host number is not changed and only one host, which means the destination has totally changed, all topologies will migrate
				if(size == 0 && hostname.size() ==1){
					System.out.println("same number of host in this queue "+i+" , and the only host has changed");
					List<ExecutorDetails> existingexecutor = getAllExecutors(queuelist.get(i), cluster);
					if(queuelist.get(i).size()>0){
						for(TopologyDetails t : queuelist.get(i)){
							Set<WorkerSlot> assignedworkerslots = getMappedWorkerSlot(cluster, t, existingexecutor);
							findSupervisorT(cluster, t, supervisors, existingexecutor, assignedworkerslots," ",false);
						}
					}
					else{
						System.out.println("no topology need to update in queue "+i);
					}
				}	
				//the host number is not changed and there are two hosts, which means there is one of the host changed
				else if(size == 0 && hostname.size() == 2){
					System.out.println("same number of host in this queue "+i+" , and there are host where one of them changed");
					List<ExecutorDetails> existingexecutor = getAllExecutors(queuelist.get(i), cluster);
					HashMap<String,String> allocatedhost = new HashMap<>();
					if(queuelist.get(i).size()>0){
						for(TopologyDetails t : queuelist.get(i)){
							HashSet<WorkerSlot> assignedworkerslots = getMappedWorkerSlot(cluster, t, existingexecutor);
							allocatedhost.put(t.getId(), findHostname(t));
							findSupervisorT(cluster, t, supervisors, existingexecutor, assignedworkerslots, allocatedhost.get(t.getId()),false);
						}
					}
					else
						System.out.println("no topology need to update in queue "+i);
				}
				// the queue has an increasing number of host, if allocated host is existed in the new allocation, decide whether move or not, if not exist, move
				else if(size == 1){
					System.out.println("increasing number of host in this queue "+i);
					List<ExecutorDetails> existingexecutor = getAllExecutors(queuelist.get(i), cluster);
					HashMap<String,String> allocatedhost = new HashMap<>();
					if(queuelist.get(i).size()>0){
						for(TopologyDetails t : queuelist.get(i)){
							HashSet<WorkerSlot> assignedworkerslots = getMappedWorkerSlot(cluster, t, existingexecutor);
							allocatedhost.put(t.getId(), findHostname(t));
							findSupervisorT(cluster, t, supervisors, existingexecutor, assignedworkerslots, allocatedhost.get(t.getId()),true);
						}
					}
					else 
						System.out.println("no topology need to update in queue "+i);
				}
				// the queue has an decreasing number of host, if allocated is existed in the new allocation, stay unchanged , otherwise, move
				else if(size == 2){
					System.out.println("decreasing number of host in this queue "+i);
					List<ExecutorDetails> existingexecutor = getAllExecutors(queuelist.get(i), cluster);
					HashMap<String,String> allocatedhost = new HashMap<>();
					if(queuelist.get(i).size()>0){
						for(TopologyDetails t : queuelist.get(i)){
							HashSet<WorkerSlot> assignedworkerslots = getMappedWorkerSlot(cluster, t, existingexecutor);
							allocatedhost.put(t.getId(), findHostname(t));
							findSupervisorT(cluster, t, supervisors, existingexecutor, assignedworkerslots, allocatedhost.get(t.getId()),false);
						}
					}
					else
						System.out.println("no topology need to update in queue "+i);
				}
				else{ 
					System.out.println("nothing to do "+size+" , "+hostname.size());
				}
				}
				else{
					System.out.println("queue "+i+ " ,has not changed schedule");
					for(TopologyDetails t : queuelist.get(i)){
						SchedulerAssignment currentAssignment = cluster.getAssignmentById(t.getId());
						Map<ExecutorDetails, WorkerSlot> assigned = new HashMap<ExecutorDetails, WorkerSlot>();
						if(currentAssignment!= null){
							assigned = currentAssignment.getExecutorToSlot();
						}
						if(assigned.size()<10){
							System.out.println("topology "+t.getId()+" has not assignment now");
							List<ExecutorDetails> existingexecutor = getAllExecutors(queuelist.get(i), cluster);
							HashSet<WorkerSlot> assignedworkerslots = getMappedWorkerSlot(cluster, t, existingexecutor);
							HashMap<String,String> allocatedhost = new HashMap<>();
							allocatedhost.put(t.getId(), findHostname(t));
							findSupervisorT(cluster, t, supervisors, existingexecutor, assignedworkerslots, allocatedhost.get(t.getId()),false);
						}
					}
				}
		}
	}

	public String findHostname(TopologyDetails t){
		//	    		ArrayList<String> names = new ArrayList<>();
		String name = "";
		HashMap<String, Integer> maps = new HashMap<>();
		if(QoS_Aware.workerlist.containsKey(t.getId())){
			if(workerlist.get(t.getId()).size()>0){
//				name = workerlist.get(t.getId()).get(0);
				int temp = 1;
				for(String s : workerlist.get(t.getId())){
					
					if(maps.containsKey(s)){
						temp = maps.get(s);
						maps.put(s, temp+1);
					}
					else
						maps.put(s, temp);
				}
				
				int max= 0;
				String frequent = "";
				for(String s : workerlist.get(t.getId())){
					if(maps.get(s)>max){
						max = maps.get(s);
						frequent = s;
					}
						
				}
				name = frequent;
			}
			
		}
		
		
		return name;
	}

	public HashSet<WorkerSlot> getMappedWorkerSlot(Cluster cluster, TopologyDetails t, List<ExecutorDetails> existingexecutor){

		HashSet<WorkerSlot> workerslots = new HashSet<WorkerSlot>();
		Map<ExecutorDetails, WorkerSlot> assigned = new HashMap<ExecutorDetails, WorkerSlot>();

		SchedulerAssignment currentAssignment = cluster.getAssignmentById(t.getId());

		if(currentAssignment!=null){
			assigned = currentAssignment.getExecutorToSlot();

			for(ExecutorDetails ed : existingexecutor){
				workerslots.add(assigned.get(ed));
				ArrayList<String> workers = new ArrayList<>();
				if(QoS_Aware.workerlist.containsKey(t.getId())){
					workers = workerlist.get(t.getId());
					if(assigned.containsKey(ed))
						workers.add(assigned.get(ed).getNodeId());
					QoS_Aware.workerlist.put(t.getId(), workers);
				}
				else{
					if(assigned.containsKey(ed))
						workers.add(assigned.get(ed).getNodeId());
					QoS_Aware.workerlist.put(t.getId(), workers);
				}
			}
		}
		return workerslots;
	}

	public List<ExecutorDetails> getExecutorTopo(TopologyDetails t, Cluster cluster, Boolean onlyneeded){
		List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
		Map<String, List<ExecutorDetails>> rest = new HashMap<>();
		Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap<>();
		if(onlyneeded){
			rest.putAll(cluster.getNeedsSchedulingComponentToExecutors(t));
			for(String s: rest.keySet()){
				if(rest.get(s).size()>0){
					executors.addAll(rest.get(s));
				}
			}
		}
		else{
			for(String sc: t.getComponents().keySet()){
				componentToExecutors.put(sc, t.getComponents().get(sc).execs);
			}
			rest.putAll(cluster.getNeedsSchedulingComponentToExecutors(t));
			
			for(String s: componentToExecutors.keySet()){
				executors.addAll(componentToExecutors.get(s));
				// remove those executors already included, try to find out all ackers and other executors
				if(rest.containsKey(s) && rest.get(s).size()>0){
					System.out.println("therer are some executors need to schedule for component "+s);
					for(ExecutorDetails ed : componentToExecutors.get(s)){
						if(rest.get(s).contains(ed)){
							rest.get(s).remove(ed);
						}
					}
				}
			}
	
			for(String s: rest.keySet()){
				if(rest.get(s).size()>0){
					executors.addAll(rest.get(s));
				}
			}
			
			}
		return executors;
	}
	public List<ExecutorDetails> getAllExecutors(ArrayList<TopologyDetails> topologies, Cluster cluster){
		List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
		// all needs schedule executors 
		Map<String, List<ExecutorDetails>> rest = new HashMap<>();


		for(TopologyDetails td: topologies){
			Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap<>();
			for(String sc: td.getComponents().keySet()){
				componentToExecutors.put(sc, td.getComponents().get(sc).execs);
			}
			rest.putAll(cluster.getNeedsSchedulingComponentToExecutors(td));

			for(String s: componentToExecutors.keySet()){
				executors.addAll(componentToExecutors.get(s));
				// remove those executors already included, try to find out all ackers and other executors
				if(rest.containsKey(s) && rest.get(s).size()>0){
					System.out.println("therer are some executors need to schedule for component "+s);
					for(ExecutorDetails ed : componentToExecutors.get(s)){
						if(rest.get(s).contains(ed)){
							rest.get(s).remove(ed);
						}
					}
				}
			}

			for(String s: rest.keySet()){
				if(rest.get(s).size()>0){
					executors.addAll(rest.get(s));
				}
			}
		}

		return executors;
	}



	public int getQueue(TopologyDetails t){
		int result = 0;
		String p = t.getName().split("_")[1];
		result = Integer.valueOf(p);
		return result;
	}

	// find the specific supervisor according to topology
	public WorkerSlot findSupervisorT(Cluster cluster, TopologyDetails topology, Collection<SupervisorDetails> supervisors, List<ExecutorDetails> exe,
			Set<WorkerSlot> assignedworkerslots, String allocatedhost, boolean increasing){
		// find out all the needs-scheduling components of this topology
		WorkerSlot w = null;
		//used for toplogy to migrate from one to another with increasing size	    	
		String tempdes = "";
		String p = String.valueOf(getQueue(topology));
		HashMap<String, ArrayList<String>> m = getList();
		ArrayList<String> assignedhost = m.get(p);
		boolean onlyrest = false;
		// the list of all nodes for the given queue
		HashMap<String, SupervisorDetails> supernodes = new HashMap<>();
		List<ExecutorDetails> exeudpate = new ArrayList<>();
		//find all supervisor that corresponds to the priority queue
		for (SupervisorDetails supervisor : supervisors) {
			if(assignedhost.contains(supervisor.getHost())){
//				supernodes.add(supervisor);
				supernodes.put(supervisor.getHost(), supervisor);
				System.out.println("adding node "+ supervisor.getHost()+" , to "+topology.getName());
			}
		}
		if (supernodes != null) {
			ArrayList<WorkerSlot> availableSlots = updateSlots(supernodes, cluster, new ArrayList<>());


			// TODO for simplicity, we free all the used slots on the supervisor.
			if (availableSlots.isEmpty() ) {
				System.out.println("no available slots in spout supervisor");
				
			}
			else{
				boolean temp = false;
				System.out.println("allocated host at the moment is "+allocatedhost);
				// not ini
				if(allocatedhost!= " "){
					System.out.println("now the nodes size is "+supernodes.size());
					onlyrest = false;
					for(SupervisorDetails sd : supernodes.values()){
						System.out.println("assigning supervisor host is "+sd.getId());
						if (sd.getId().equals(allocatedhost)){
							temp = true;
							System.out.println("this topology "+topology.getId()+" ,already at the destinated host "+allocatedhost);
						}
						else{
							System.out.println(" not euqal "+allocatedhost + " , "+sd.getId());
						}
						
					}
				}
				HashMap<String, SupervisorDetails> nodesupdate = supernodes;
				ArrayList<String> orderinghsot = nodesReorder(supernodes, cluster);
				System.out.println("checking the ordering host now is "+orderinghsot.toString());
					// need to justify
				if(temp){
					
					// the queue host increased, need to decide whether to move
					if(increasing == true)
						{
						onlyrest = false;
//						HashMap<String, SupervisorDetails> nodesupdate = supernodes;
//						HashMap<String,Integer> status = nodesReorder(supernodes, cluster);\
//						ArrayList<String> orderinghsot = nodesReorder(supernodes, cluster);
//						System.out.println(nodesupdate.size());
//						int max = 0;
//						String firstone = "";
//						for(String s: status.keySet()){
//							if(status.get(s)>max){
//								max = status.get(s);
//								firstone = s;
//							}
//						}
//						System.out.println("node update with "+supernodes.get(firstone).getId()+" as the first one");
						// if the existing destination is the one with most slots, stay unchanged)
						
						
						String firstone = orderinghsot.get(0);
					if(nodesupdate.containsKey(firstone)){
						if(allocatedhost.equals(nodesupdate.get(firstone).getId())){
							System.out.println("topology "+topology.getId()+" will not change the destinaion");
							if(exeudpate.size() != 0){
								tempdes = firstone;
								System.out.println("still have some executors to map");
								onlyrest =true;
							}
						}
					}
						else{
							tempdes = firstone;
							System.out.println("the topology "+topology.getId()+" needs to migrate to "+tempdes);
							onlyrest = false;
							}
						}
					
					}
//					HashMap<String, SupervisorDetails> nodesupdate = supernodes;
					System.out.println("need to reassign "+topology.getId());
				
			
//					exe = getExecutorTopo(topology, cluster);
					
					exeudpate = getExecutorTopo(topology, cluster, onlyrest);
					cluster.freeSlots(assignedworkerslots);
					System.out.println("size is "+exeudpate.size());
					int remain = exeudpate.size()%4;
					int templength = exeudpate.size()/4;
					System.out.println("temp length is "+templength+" ,and remain is "+remain);
					int index = 0;	
					HashMap<String, SupervisorDetails> templist = new HashMap<>();
					if(tempdes!=""){
						System.out.println("already find the destination with "+tempdes);
						templist.put(tempdes, supernodes.get(tempdes));
					}
					for(int i =0; i<4; i++){
//						System.out.println("insdie assing loop the size is "+templist.size());
						if(tempdes!="" && !updateSlots(templist, cluster,orderinghsot).isEmpty()){
							w = updateSlots(templist, cluster,orderinghsot).get(0);
						}
						else {
							if(!updateSlots(nodesupdate, cluster,orderinghsot).isEmpty())
								w = updateSlots(nodesupdate, cluster,orderinghsot).get(0);
							else{
								System.out.println("w is null");
							}
						}
						ArrayList<ExecutorDetails> part = new ArrayList<ExecutorDetails>();
						for(int j = index; j<index+templength; j++){
							part.add(exeudpate.get(j));
						}
						index = index+templength;
						System.out.println("executors are "+part.toString());
						if(w!=null){
							cluster.assign(w, topology.getId(), part);
							System.out.println("assign topology "+topology.getName()+ " , to "+w.getNodeId()+" , "+w.getPort());
						}
						else
							System.out.println("not finding the destinaiton");
						}
					if(remain >0){
						if(!updateSlots(nodesupdate, cluster,orderinghsot).isEmpty())
							w = updateSlots(nodesupdate, cluster,orderinghsot).get(0);
						ArrayList<ExecutorDetails> part = new ArrayList<ExecutorDetails>();
						for(int i = index; i<index+remain; i++){
							part.add(exeudpate.get(i));
						}
						System.out.println("executors are "+part.toString());
						if(w!=null){
							cluster.assign(w, topology.getId(), part);
							System.out.println("assign topology "+topology.getName()+ " , to "+w.getNodeId()+" , "+w.getPort());
						}
						else{
							System.out.println("cannot find avaialble port in "+w.getNodeId());
						}
						}
				}
				
			}
		return w;
	}
		
	

	public ArrayList<String> nodesReorder (HashMap<String, SupervisorDetails> nodes, Cluster cluster){
		//for testing
		ArrayList<String> result = new ArrayList<>();
		if(nodes.size()>1){
			for(SupervisorDetails sd : nodes.values()){
				if(sd.getHost().contains("l")){
					result.add(sd.getHost());
				}
			}
			for(SupervisorDetails sd : nodes.values()){
				if(sd.getHost().contains("m")){
					result.add(sd.getHost());
				}
			}
			for(SupervisorDetails sd : nodes.values()){
				if(sd.getHost().contains("s")){
					result.add(sd.getHost());
				}
			}
		}
		else{
			for(SupervisorDetails sd : nodes.values()){
					result.add(sd.getHost());
			}
		}
		return result;
	}
	
	
	
	// update the slot according to the nodes size
//	public HashMap<String, Integer> nodesReorder (HashMap<String, SupervisorDetails> nodes, Cluster cluster){
//		//		ArrayList<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
//		HashMap<String,Integer> updatelist = new HashMap<>();
//	
//		}
//		
//		
//		if(nodes.size()>1){
////			int[] avail = new int[nodes.size()];
//			HashMap<String, Integer> avail = new HashMap<>();
////			int minind = 0;
////			int maxind = nodes.size()-1;
////			for(int i = 0 ; i<nodes.size(); i++){
////				avail[i] = numofSlots(nodes.get(i), cluster);
////				//  							System.out.println("index "+i+", with number of slot "+avail[i]);
////				if(avail[i]<avail[minind])
////					minind = i;
////				if(avail[i]>=avail[maxind])
////					maxind = i;
////			}
//			int min = Integer.MAX_VALUE;
//			int max = Integer.MIN_VALUE;
//			String mi = "";
//			String ma = "";
//			for(String s : nodes.keySet()){
//				avail.put(s, numofSlots(nodes.get(s), cluster));	
//			}
////			System.out.println("inside nodesreorder the size is "+avail.size());
//			for(String s : avail.keySet()){
//				if(avail.get(s)<min){
//					min = avail.get(s);
//					mi = s;
//				}
//				if(avail.get(s)>max){
//					max = avail.get(s);
//					ma = s;
//				}
//			}
//			System.out.println("now the min is "+min+" with "+mi+" and the max is "+max+ " with "+ma);
//			if(avail.size()==3){
//				updatelist.put(ma , max);
////				updatelist.add(nodes.get(maxind));
//				for(String s: nodes.keySet()){
//					if(!s.equals(mi) && !s.equals(ma))
//						updatelist.put(s, avail.get(s));
//				}
////				for(int j = 0; j<3; j++){
////					if(j!=maxind && j!= minind)
////						updatelist.add(nodes.get(j));
////				}
////				updatelist.add(nodes.get(minind));
//				updatelist.put(ma, max);
//			}
//			else{
//				//  							System.out.println("minind "+minind+ " , maxind "+maxind);
////				updatelist.add(nodes.get(maxind));
////				updatelist.add(nodes.get(minind));
//				updatelist.put(ma, max);
//				updatelist.put(mi, min);
//			}		
//		}
//		return updatelist;
//	}

	// return the number of avail
	public int numofSlots(SupervisorDetails node, Cluster cluster){
		ArrayList<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
		for(WorkerSlot ws : cluster.getAvailableSlots(node)){
			availableSlots.add(ws);
		}
		return availableSlots.size();
	}

	/**
	 * update the available slot
	 * @param nodes
	 * @return
	 */
	public ArrayList<WorkerSlot> updateSlots(HashMap<String, SupervisorDetails> nodes, Cluster cluster, ArrayList<String> orderingnodes){
		ArrayList<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
	
		if(nodes.size()>0 && orderingnodes.size()>0){
//			for(String s : nodes.keySet()){
			System.out.println("check the status of updated list");
			for(int i = 0; i<orderingnodes.size();i++){
				//					availableSlots.addAll(cluster.getAvailableSlots(sd))
				if(cluster.getAvailableSlots(nodes.get(orderingnodes.get(i))).isEmpty()){
					System.out.println("no enough space at supervisor "+orderingnodes.get(i));
				}
				else{
					for(WorkerSlot ws : cluster.getAvailableSlots(nodes.get(orderingnodes.get(i)))){
						availableSlots.add(ws);
					}
				}
			}
		}
		else if(nodes.size()>0){
			for(String s : nodes.keySet()){
				if(cluster.getAvailableSlots(nodes.get(s)).isEmpty()){
					System.out.println("no enough space at supervisor "+s);
				}
				else{
					for(WorkerSlot ws : cluster.getAvailableSlots(nodes.get(s))){
						availableSlots.add(ws);
					}
				}
			}
		}
		else{
			System.out.println("no availble slot in this updated list of nodes");
		}
		return availableSlots;
	}

	/**
	 * Reading the host for each queue from the file
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
		HashMap<String, ArrayList<String>> mapping = new HashMap<>();
		ArrayList<String> temp;
		try{
			reader = new FileReader(file);
			br = new BufferedReader(reader);
			//				TopologyScheduler ts = null;
			if(br.readLine() == null)
				reschedule = false;
			while((line=br.readLine())!=null){
				//the file format should be 1 [s1,m1]
				if(line.length()>1){
//					System.out.println("reading line "+line);
					String pri = line.substring(0, 1);
					String size = line.substring(2,3);
					int indl = line.indexOf('[');
					int indr = line.indexOf(']');
					String update ="false";
					int indexupdate = line.indexOf('(');
					int indexupdateright = line.indexOf(')');
					if(indexupdateright>indexupdate)
						update = line.substring(indexupdate+1,indexupdateright);
					//				System.out.println("reading pri "+pri+" size "+size+" "+update);
					QoS_Aware.queueupdate.put(Integer.valueOf(pri), Boolean.valueOf(update));
					QoS_Aware.queueusize.put(pri, Integer.valueOf(size));
					String[] t = line.substring(indl+1, indr).split(",");
					temp = new ArrayList<String>();
					for(String s: t){
						temp.add(s);
					}
					mapping.put(pri, temp);
					//				System.out.println("also put record "+temp.toString());
				}
			}
		}
		catch(FileNotFoundException e){
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		setList(mapping);
		//			setActivenum(convertReading(temp));
		return reschedule;
	}

	public HashMap<String, ArrayList<String>> getList() {
		return list;
	}

	public void setList(HashMap<String, ArrayList<String>> list) {
		this.list = list;
	}


}


