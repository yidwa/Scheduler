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
	ArrayList<PriorityQueue> pq;
	HashMap<String, LinkedList<Double>> arr;
	HashMap<String, LinkedList<Double>> ser;
	
	HashMap<Integer, ArrayList<String>> mappingresult;
	HashMap<Integer, Integer> mappingsize;
	HashMap<Integer, Boolean> mappingupdate;

	
	// distinguish two types of QoS scheduling ,regarding throughpu or latency
	boolean qoslat;
	double qos;
	double cpu;
	double swi;

	public QueueUpdate(StormREST sr,HashMap<String, Topology>  topologies, HashMap<String, Integer> priority, ArrayList<PriorityQueue> pq,
			HashMap<String, LinkedList<Double>> arr, HashMap<String, LinkedList<Double>> ser, boolean latqos,
			double qos, double cpu, double swi) {
//		 TODO Auto-generated constructor stub
		this.topologies = topologies;
		this.priority = priority;
		this.pq = pq;
		this.arr = arr;
		this.ser = ser;
		this.mappingresult = new HashMap<>();
		this.mappingsize = new HashMap<>();
		this.mappingupdate = new HashMap<>();	
		this.qoslat = latqos;
		this.qos = qos;
		this.cpu = cpu;
		this.swi = swi;
	}
	
	  public void updateLatency(ArrayList<PriorityQueue> pq , int pri, LinkedList<Double> arr, LinkedList<Double> serv){
		 
	
		  	PriorityQueue q = pq.get(pri-1);

		  	q.setArr(arr);
		  	q.setServ(serv);
		  	int size = hostofQueue(q);
		  	q.getQl().setNumChannel(size);
		  	q.getQl().setArrivalPt(updateLA(arr));
		  	q.getQl().setServicePt(updateLA(serv));
		  	q.getQl().meanarrv = QueueLatency.meanUpdate(arr);
		  	q.getQl().meanserv = QueueLatency.meanUpdate(serv);
//		  	double estimation = q.getQl().waittimeEstimating(size);
		  	double buffertimetotal = 0.0;
		  	for(String s: q.getBuffertime().keySet()){
		  		buffertimetotal += q.getBuffertime().get(s);
		  	}
		  	double bufferaverage = buffertimetotal/q.getNames().size();
		  	bufferaverage = Double.valueOf(Methods.formatter.format(bufferaverage));
		  	q.setAvgbuf(bufferaverage);
//		  	System.out.println("set buffer time for "+q.getPrioirty()+" as "+bufferaverage);
//		  	q.setWaittime(estimation);
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
	  
	  /**
	   * update the host of the queue and return the size of host
	   * @param pq
	   * @return
	   */
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
			  }
		  }
		  pq.setHosts(hostupdate);
		  
		  int size =  hosts.size();
		  return size;
	  }
	
/**
 * run periodically 
 */
		public void queueMetric(){
		
//			LinkedList<Double> temparr = new LinkedList<>();
//			LinkedList<Double> tempserv = new LinkedList<>();
		
			for(PriorityQueue p : pq){
//				temparr = p.getArr();
//				tempserv = p.getServ();
			
				if(p.getSize()>0){
//					updateLatency(pq, p.getPrioirty(), temparr, tempserv);
				
					// the existing scheduling host
					System.out.println("existing scheduling info for priority "+p.getPrioirty()+ " : "+p.getHosts().toString());
					ArrayList<String> queuemapping = new ArrayList<>();
					if(p.getQl().meanserv<100 && p.getQl().meanserv != 0)
						queuemapping = QoS_Opt.optimizedSolution(p, topologies, qoslat, qos, cpu, swi);
					else{
						System.out.println("the queue "+p.getPrioirty()+" is not stable yet, with mean service rate as "+p.getQl().meanserv);
						queuemapping = p.getHosts();
					}
					
					System.out.println("the derived scheduling decision "+queuemapping.toString());
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
			Methods.writeFile(mappresult, "qos_schedule",false);
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
						sen += s+";"+emit+";"+lat+";"+fail;
					}
					sen+=",";
				}
				sen+="\n";
			}
			Methods.writeFile(sen, "queue_metrics.txt",true);
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
//		// TODO Auto-generated method stub
		System.out.println("new Queueupdate starts");
		queueMetric();
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
