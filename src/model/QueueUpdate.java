package model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.storm.scheduler.TopologyDetails;

import general.Component;
import general.Executor;
import general.Methods;
import general.PriorityQueue;
import general.StormREST;
import general.Topology;
import opt.Optimisation;
import opt.QoS_Opt;

public class QueueUpdate implements Runnable {
	
	HashMap<String, Topology> topologies;
	HashMap<String, Integer> priority;
//	HashMap<String, Throughput> throughput;
//	HashMap<String, Latency> latency;
//	HashMap<String, Metrics> metrics;
	ArrayList<PriorityQueue> pq;
	HashMap<String, LinkedList<Double>> arr;
	HashMap<String, LinkedList<Double>> ser;
	
	HashMap<Integer, ArrayList<String>> mappingresult;
	HashMap<Integer, Integer> mappingsize;
	HashMap<Integer, Boolean> mappingupdate;

	public QueueUpdate(StormREST sr,HashMap<String, Topology>  topologies, HashMap<String, Integer> priority, ArrayList<PriorityQueue> pq, 
			HashMap<String, LinkedList<Double>> arr, HashMap<String, LinkedList<Double>> ser) {
//		 TODO Auto-generated constructor stub
		this.topologies = topologies;
		this.priority = priority;
		this.pq = pq;
//		this.throughput = new HashMap<String,Throughput>();
//		this.latency = new HashMap<String,Latency>();
//		this.metrics = new HashMap<String, Metrics>();
		this.arr = arr;
		this.ser = ser;
		this.mappingresult = new HashMap<>();
		this.mappingsize = new HashMap<>();
		this.mappingupdate = new HashMap<>();
//		for(Topology t: topologies.values()){
//			Throughput thr = new Throughput(t.getTid(), t.getCompostruct(), t.getCompo());
//			Latency lc = new Latency(t.getTid(), t.getCompostruct(),t.getCompo(), new ArrayList<Double>(), new ArrayList<Double>(), t.getTworker().size());
			
//			this.throughput.put(t.getTid(), thr);
//			this.latency.put(t.getTid(), lc);
//			this.metrics.put(t.getTid(), new Metrics(t.getTid(), 0, 0));
//		}
		
	}
	
	  public void updateLatency(ArrayList<PriorityQueue> pq , int pri, LinkedList<Double> arr, LinkedList<Double> serv){
		 
		  	PriorityQueue q = pq.get(pri-1);
//		 	System.out.println("now update latency for"+ pri+ " ,it's size is "+ q.getSize());
//		 	System.out.println("the size for arr is "+arr.size() +" , the size for serv is "+serv.size());
		 	
		  	q.setArr(arr);
		  	q.setServ(serv);
		  	int size = hostofQueue(q);
//		  	q.setSize(size);
		  	q.getQl().setNumChannel(size);
		  	q.getQl().setArrivalPt(updateLA(arr));
		  	q.getQl().setServicePt(updateLA(serv));
		  	
//		  	System.out.println("priority "+q.getPrioirty()+" has the size of "+size);
		  	double estimation = q.getQl().waittimeEstimating(size);
		  	double buffertimetotal = 0.0;
		  	for(String s: q.getBuffertime().keySet()){
		  		buffertimetotal += q.getBuffertime().get(s);
		  	}
		  	double bufferaverage = buffertimetotal/q.getNames().size();
		  	q.setAvgbuf(bufferaverage);
		  	//not including the time for execution
		  	q.setWaittime(estimation);
//		  	System.out.println("the waiting time for queue "+pri +" just udpated with estimation "+estimation+ " and the buffertime averaget "+bufferaverage);
	    }
		
//	    public void updatethroughput(String tname, HashMap<String, ArrayList<Double>> cp, HashMap<String, ArrayList<Double>> ce,HashMap<String, Long> lp, HashMap<String, Long> lc){
//	    	throughput.get(tname).updateData(cp, ce, lp, lc);
//	    }
	    
	  public ArrayList<Double> updateLA(LinkedList<Double> ll){
		  ArrayList<Double> result = new ArrayList<Double>();
		  for(Double d:ll){
			  result.add(d);
		  }
		  return result;
	  
	  }
	  
	  public int hostofQueue(PriorityQueue pq){
		  Set<String> hosts = new HashSet<String>();
		  
		  // topologies belong to the queue
		  ArrayList<String> ts = pq.getNames();
		  ArrayList<Executor> exes = new ArrayList<Executor>();
		  for(String t: ts){
			  if(topologies.containsKey(t)){
				  Topology topo = topologies.get(t);
				  exes = topo.getTworker();
				  for(Executor e : exes){
					  hosts.add(e.getHost());
				  }
			  }
		  }
		  ArrayList<String> hostupdate = new ArrayList<String>();
		  if(hosts.size()>0){
			  for(String s: hosts){
				  hostupdate.add(s);
//			  System.out.println("udpate queue "+pq.getPrioirty()+" with host "+s);
			  }
		  }
		  pq.setHosts(hostupdate);
		  
		  int size =  hosts.size();
		  return size;
	  }
	
/**
 * update the waiting time for each queue
 */
		public void performanceMetric(){
		
			LinkedList<Double> temparr = new LinkedList<>();
			LinkedList<Double> tempserv = new LinkedList<>();
		
			for(PriorityQueue p : pq){
				System.out.println("queue for "+ p.getPrioirty()+" ,size is "+p.getSize());
				
				temparr = p.getArr();
				tempserv = p.getServ();
			
//				System.out.println("update latency for "+p.getPrioirty());
				if(p.getSize()>0){
					updateLatency(pq, p.getPrioirty(), temparr, tempserv);
				
					System.out.println("before optimizaiton ");
					System.out.println(p.getHosts().toString());
					ArrayList<String> queuemapping = QoS_Opt.optimizedSolution(p, topologies);
					System.out.println("after optmization ");
					System.out.println(queuemapping.toString());
					// mapping changed 
					if(!compareArrays(p.getHosts(), queuemapping)){
						System.out.println("mapping changed for queue "+p.getPrioirty());
						mappingresult.put(p.getPrioirty(), queuemapping);
						if(p.getHosts().size()<queuemapping.size())
							mappingsize.put(p.getPrioirty(), 1);
						else if(p.getHosts().size()>queuemapping.size())
							mappingsize.put(p.getPrioirty(), 2);
						else
							mappingsize.put(p.getPrioirty(), 0);
						
						mappingupdate.put(p.getPrioirty(), true);
					}
					else{
						System.out.println("mapping not changed for queue "+p.getPrioirty());
						mappingresult.put(p.getPrioirty(), queuemapping);
//						System.out.println("nochange 1");
						mappingsize.put(p.getPrioirty(), 0);
//						System.out.println("nochange 2");
						mappingupdate.put(p.getPrioirty(), false);
//						System.out.println("nochange 3");
//						System.out.println("mapping unchanged for queue "+ p.getPrioirty());
					}
				}
				//no topology
				else{
					ArrayList<String> temp = new ArrayList<>();
					temp.add("m"+p.getPrioirty());
					mappingresult.put(p.getPrioirty(), temp);
					mappingsize.put(p.getPrioirty(), 0);
					mappingupdate.put(p.getPrioirty(), false);
				}
			}
//			System.out.println("before updating mapping result "+mappingsize.size()+" "+mappingupdate.size()+" "+mappingresult.size());
			String mappresult = "";
			for(int i : mappingresult.keySet()){
				mappresult += i+" "+mappingsize.get(i)+" ("+mappingupdate.get(i)+")"+" "+mappingresult.get(i).toString()+"\n";
			}
//			System.out.println("mapping result "+mappresult);
			Methods.writeFile(mappresult, "schedule",false);
			Methods.writeFile(mappresult, "history", true);
			String sen = "";
			for(PriorityQueue p : pq){
				ArrayList<String> tsinp = p.getNames();
				sen += p.getPrioirty()+",";
//				+","+p.getAvgbuf()+","+p.getWaittime()+","+p.getNames().toString();
				for(String s : topologies.keySet()){
					if(tsinp.contains(s)){
						Topology t = topologies.get(s);
						double emit = t.getSystememit();
						long fail = t.getFailed();
						double lat = t.getSystemlatency();
						sen += s+","+emit+","+lat+","+fail;
					}
					sen+=",";
				}
				sen+="\n";
			}
			Methods.writeFile(sen, "metrics.txt",true);
//			    System.out.println("set metrics "+ thr+ " , "+ lat);
//			    System.out.println("metric of "+s+" , "+metrics.get(s).latency+" , "+metrics.get(s).throughput);
		}
			   /**
			    * return true if two arrays equal
			    * @param a
			    * @param b
			    * @return
			    */
		public static boolean compareArrays(ArrayList<String> a, ArrayList<String> b){
			boolean result = true;
			if(a==null && b==null)
				return true;
			if((a==null && b!= null)||(a!=null && b==null))
				return false;
			if(a.size()!=b.size())
				return false;
			else{
				for(String d: a){
					if(!b.contains(d))
						return false;
				}
//				for(double d:b){
//					if(!a.contains(d))
//						return false;
//				}
			}
			return result;
		}
		
	@Override
	public void run() {
//		System.out.println("no metric update ");
//		// TODO Auto-generated method stub
		System.out.println("new Queueupdate starts");
		performanceMetric();
	}
	
	
	
	public HashMap<String, Topology> getTopologies() {
		return topologies;
	}

	public void setTopologies(HashMap<String, Topology> topologies) {
		this.topologies = topologies;
	}

//	public HashMap<String, Throughput> getThroughput() {
//		return throughput;
//	}
//
//	public void setThroughput(HashMap<String, Throughput> throughput) {
//		this.throughput = throughput;
//	}
//
//	public HashMap<String, Latency> getLatency() {
//		return latency;
//	}
//
//	public void setLatency(HashMap<String, Latency> latency) {
//		this.latency = latency;
//	}
//
//	public HashMap<String, Metrics> getMetrics() {
//		return metrics;
//	}
//
//	public void setMetrics(HashMap<String, Metrics> metrics) {
//		this.metrics = metrics;
//	}
}
