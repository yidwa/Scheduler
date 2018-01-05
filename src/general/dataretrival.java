package general;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class dataretrival implements Runnable {

	public HashMap<String, LinkedList<Double>> arr;
	public HashMap<String, LinkedList<Double>> ser; 
	public StormREST sr;
	public HashMap<String,Topology> topologies;
	public HashMap<String,Integer> priority;
	public ArrayList<PriorityQueue> queues;
	public HashMap<Integer, LinkedList<Double>> arrtemp;
	public HashMap<Integer, LinkedList<Double>> servtemp;
	//used for calculation temporarly
	public ArrayList<ArrayList<Double>> queuearr;
	public ArrayList<ArrayList<Double>> queueser;
	
	/**
	 * retrieve data for data incoming and service 
	 * @param t
	 * @param p
	 * @param arr
	 * @param ser
	 * @param sr
	 */
	public dataretrival(HashMap<String,Topology> t, HashMap<String, Integer> p, HashMap<String, LinkedList<Double>> arr,
			HashMap<String, LinkedList<Double>> ser, StormREST sr, ArrayList<PriorityQueue> pq){
		this.arr = arr;
		this.ser = ser;
		this.sr = sr;
		this.topologies = t;
		this.priority = p;
		this.queues = pq;
//		this.arrtemp = new HashMap<Integer, LinkedList<Double>>();
//		this.servtemp = new HashMap<Integer, LinkedList<Double>>();
//		iniTemp(pq);
		this.queuearr = new ArrayList<ArrayList<Double>>();
		this.queueser = new ArrayList<ArrayList<Double>>();
		iniQueue(pq);
	}
	/**
//	 * Initialize the temp values for each queue
//	 * @param pq
//	 */
//	public void iniTemp(ArrayList<PriorityQueue> pq){
//			for(PriorityQueue p : pq){
//				arrtemp.put(p.getPrioirty(), p.getArr());
//				servtemp.put(p.getPrioirty(), p.getServ());
//			}
//	}
	
	/**
	 * Initialize the arr and serv for each queue
	 */
	public void iniQueue(ArrayList<PriorityQueue> pq){
		for(int i = 0 ;i<pq.size();i++){
//			Double d = 0.0; 
			ArrayList<Double> d = new ArrayList<Double>();
			queuearr.add(d);
			ArrayList<Double> ad = new ArrayList<Double>();
			queueser.add(ad);
		}
	}
	/**
	 * update the incoming rate of each topology and the processing rate
	 * @param sr
	 */
	public void updateRates(StormREST sr){
		sr.Topologyget(false);
//		sr.Topologyget(topologies,priority, false,queues);
		sr.Topologyinfo(topologies);
		double arrv = 0;
		double serv = 0; 

		
		for(Topology t : topologies.values()){
			String spoutname ="";
			for(Component c: t.getCompo().values()){
				if(c.spout == true)
					spoutname = c.cid;
			}
			
			arrv = sr.freqInfo(t.tid, spoutname, topologies);
			//testing purpose
			System.out.println("now the emit rate for "+t.tname+" is "+arrv);
			// reserve for the model-based scheduling
//			serv = sr.serviceRate(t.tid,topologies);
			// this is for the execution for priority based scheduling
			int pri = priority.get(t.tid);
//			System.out.println("inside update ratese in dataretvieal and the priority is "+pri);
			serv = sr.executionTotaltime(t.tid, topologies, queues.get(pri-1));
			System.out.println("get "+t.tid+ " arrv is "+arrv+" , serv "+serv);
			//update the traffic monitor for queues
			
			ArrayList<Double> temp = queuearr.get(pri-1);
			temp.add(arrv);
//			temp += arrv;
//			double update = updateArrivalTime(queuearr.get(pri-1 ));
//			System.out.println("new observed value for arrival is "+arrv+" ,the original one is "+temp+" ,the update value is "+update);
//			
			queuearr.set(pri-1, temp);
			
		    // add average service rate and do the average at last
			ArrayList<Double> tempserv = queueser.get(pri-1);
			tempserv.add(serv);
			
			
			if(arr.containsKey(t.tid)){
				 	if(arr.size()==10){
				 		arr.get(t.tid).removeFirst();
				 		ser.get(t.tid).removeFirst();
				 	}
				 		arr.get(t.tid).addLast(arrv);
				 		ser.get(t.tid).addLast(serv);
				}
			else{
//				System.out.println("adding new entry in arr and ser for "+t.tid);
				LinkedList<Double> ll = new LinkedList<>();
				LinkedList<Double> ls = new LinkedList<>();
				ll.addFirst(arrv);
				ls.addFirst(serv);
				arr.put(t.tid, ll);
				ser.put(t.tid, ls);
			}
			
//			System.out.println("the arrival rate now is "+arr.toString());
//			System.out.println("the service rate now is "+ser.toString());
		}
		System.out.println("update has been done for all topologies");
		// the update has been done for all topologies
	
		
		
//		only used for queue scheduling
		for(PriorityQueue p : queues){
			if(p.getSize() != 0){
				if(p.getArr().size() == 10){
					p.getArr().removeFirst();
					p.getServ().removeFirst();
				}
				int max;
//				double temp = queuearr.get(p.getPrioirty()-1);
//				System.out.println("inside Update rate for arrival "+queuearr.get(p.getPrioirty()-1));
//				p.getArr().addLast(queuearr.get(p.getPrioirty()-1));
				double arrupdate = updateArrivalTime(queuearr.get(p.getPrioirty()-1));
				p.getArr().addLast(arrupdate);
//				System.out.println("update arr for queue "+p.getPrioirty()+ " with "+queuearr.get(p.getPrioirty()-1));
				double sum = 0 ;
				for(Double d: queueser.get(p.prioirty-1)){
					sum += d;
				}
				double avg = sum/(queueser.get(p.prioirty-1).size());
				p.getServ().addLast(avg);
				System.out.println("update arr for queue "+p.getPrioirty()+ " with "+arrupdate +" update service rate as "+avg );
			}
		}
		
		
	}
	
	public double updateArrivalTime(ArrayList<Double> list){
		double result = 0;
		double max = 0;
		
		for(Double d: list){
			if(d>max)
				max =d;
		}
		
		for(Double d :list){
			result += max/d;
		}
//		double temp = Math.max(newobser, ori);
//		double a = temp/newobser;
//		double b = temp/ori;
//		result = Double.valueOf(Methods.formatter.format(temp/(a+b)));
		result = Double.valueOf(Methods.formatter.format(max/(result)));
		return result;
	}
	public HashMap<Integer, LinkedList<Double>> getArrtemp() {
		return arrtemp;
	}

	public void setArrtemp(HashMap<Integer, LinkedList<Double>> arrtemp) {
		this.arrtemp = arrtemp;
	}


	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("new datareteival start");
		updateRates(sr);
//		performanceMetric();
	}
}
	
	// only used for cpu scheduler on a component basis
	/**
	 * write to the metrics.txt for recording system metric, including topology emit, complete latency, fail rate,
	 * cid, component tranfer, component latency, 
	 */
//	public void performanceMetric(){
////		System.out.println("new performance metric update");
//		String sen = "";
//		for(String s: topologies.keySet()){
//			sen += s+", ";
//			 sen += ", "+ topologies.get(s).getSystememit()+", "+topologies.get(s).getSystemlatency()+", "+topologies.get(s).getFailrate() +", ";
//		    for(String cid : topologies.get(s).getCompo().keySet()){
//		    	Component c = topologies.get(s).getCompo().get(cid);
//		    	long pro = c.getLasttrans();
//		    	double latency = c.getExeLatency();
//		    	double processing = c.getProcLatency();
////		    	double la = c.getModellatency();
//		    	sen += cid+", "+pro+", "+latency+", "+processing+", ";
////		    	System.out.println("component thread size "+c.getThreads().size());
//		    	for(ComponentThread ct : c.getThreads().values()){
//		    		String hostport = ct.getExecutor().getIndex();
//		    		double threadlatency = ct.getExecutelatency();
//		    		double threadProlatency = ct.getProcesslatency();
//		    		long threadthr = ct.getExecute();
//		    		sen += hostport+", "+threadthr + ", "+threadlatency+", "+threadProlatency+", ";
//		    	}
//		    }
//		    sen += "\n";
//		}
//		Methods.writeFile(sen, "details_metrics.txt",true);
//	}
//}


//	public static void main(String[] args) throws InterruptedException, IOException {
	
	
		
		
//		StormREST sr = new StormREST("http://115.146.85.187:8080");

	//	String freq = args[0];
	
//		sr.Topologyget();
//		dataretrival.getArrvialRate(sr);
//		System.out,.println(sr.topologies);

//	
//	}	
//		}
//		sr.Supervisorinfo();
//		sr.Topologyinfo();
////	
//		sr.topologySum();
//		
		
//	for(Entry<String, Topology> e : sr.topologies.entrySet()){
//		System.out.println(e.getKey()+ " , "+ sr.topologies.get(e.getKey()).tworker.toString());
//	}
	//sr.Supervisorinfo();
//	System.out.println(sr.topologies.toString());
		
//	ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
//	exec.scheduleAtFixedRate(new Runnable() {
//		public void run(){
////				sr.Supervisorinfo();
//			PerformanceUpdate pu = new PerformanceUpdate(sr.topologies, sr.hostport);
//			pu.updating();
//			}
//			
//		}, 0, 5, TimeUnit.MINUTES);
//	}

	
	//for testing purpose, run 5 times with delay of 10 seconds
//	ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(5);
//	
//
//
////////
////////	
//	for (int i = 0; i<3; i++){
//		
//		dataretrival dt = new dataretrival();
//		
////		PerformanceUpdate pu = new PerformanceUpdate(sr.topologies, sr.hostport);
////////		SuperVisorUpdate update = new SuperVisorUpdate(sr.workers, sr.hostport);
//		scheduledPool.schedule(dt, 0, TimeUnit.SECONDS);
//////		scheduledPool.scheduleAtFixedRate(pu, 0, 20, TimeUnit.SECONDS);
//		System.out.println("new thread start");
//		Thread.sleep(5*1000);
//		}
//		
//	
////
////	
////////		
//	Threads.sleep(3000);
////	
//	scheduledPool.shutdown();
////	
//	while(!scheduledPool.isTerminated()){
//		}
////	
//	System.out.println("all finished");
////
//	}
//	}
//}

