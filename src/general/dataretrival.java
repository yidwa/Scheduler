package general;


import java.util.HashMap;
import java.util.LinkedList;

import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

import model.Metrics;


public class dataretrival implements Runnable {

	public HashMap<String, LinkedList<Double>> arr;
	public HashMap<String, LinkedList<Double>> ser; 
	public StormREST sr;
	public HashMap<String,Topology> topologies;
	
	public dataretrival(HashMap<String,Topology> t, HashMap<String, LinkedList<Double>> arr,HashMap<String, LinkedList<Double>> ser, StormREST sr){
		this.arr = arr;
		this.ser = ser;
		this.sr = sr;
		this.topologies = t;
//		System.out.println("dataretreiveal constructur");
	}
	
	
	public void updateRates(StormREST sr){
		sr.Topologyget(topologies, false);
		//sr.topologySum();
		sr.Topologyinfo(topologies);
		double arrv = 0;
		double serv = 0; 
//		System.out.println("data retrival test A "+topologies.size());
		for(Topology t : topologies.values()){
//			System.out.println("update arr and serv for topoloyg "+t);
//			String s = t.getSpout().id;
			String s ="";
			for(Component c: t.getCompo().values()){
				if(c.spout == true)
					s = c.cid;
			}
//			System.out.println("data retrival test B ");
//			System.out.println("s is "+s);
			arrv = sr.freqInfo(t.tid, s, topologies);
//			System.out.println("data retrival test BB ");
			serv = sr.serviceRate(t.tid,topologies);
//			System.out.println("data retrival test C "+arr.size());
//			System.out.println("tid "+ t.tid + "  s "+s);
//			System.out.println("arrival rate is "+arrv);
			if(arr.containsKey(t.tid)){
//				System.out.println("already contained");
				 	if(arr.size()==10){
				 		arr.get(t.tid).removeFirst();
				 		ser.get(t.tid).removeFirst();
				 	}
//				 	//get the delta value of latest observation and last value
//				 		arrv = arrv - arr.get(t.tid).get(arr.get(t.tid).size()-1);
//				 		serv = serv - ser.get(t.tid).get(ser.get(t.tid).size()-1);
				 		arr.get(t.tid).addLast(arrv);
				 		ser.get(t.tid).addLast(serv);
//				 	System.out.println(arr.toString());
				}
			else{
				System.out.println("adding new entry in arr and ser for "+t.tid);
				LinkedList<Double> ll = new LinkedList<>();
				LinkedList<Double> ls = new LinkedList<>();
				ll.addFirst(arrv);
				ls.addFirst(serv);
				arr.put(t.tid, ll);
				ser.put(t.tid, ls);
				
//				System.out.println("new created "+ arr.containsKey(t.id));
			}
//			for(Component c: topologies.get(t.tid).getCompo().values()){
//				sr.updateComponentThread(t.tid, c.cid,topologies, false);
//			}
		}
	}
//	public HashMap<String, LinkedList<Double>> getArr() {
//		return arr;
//	}
//
//	public HashMap<String, LinkedList<Double>> getSer() {
//		return ser;
//	}


	@Override
	public void run() {
		// TODO Auto-generated method stub
		
//		sr.Topologyget();
		System.out.println("new datareteival start");
		updateRates(sr);
		performanceMetric();
	
	}
	
	public void performanceMetric(){
		System.out.println("new performance metric update");
		String sen = "";
//		System.out.println("topology size "+topologies.keySet().size());
		for(String s: topologies.keySet()){
			sen += s+"\n";
			 sen += "system states "+ topologies.get(s).getSystememit()+" , "+topologies.get(s).getSystemlatency()+"\n";
//			System.out.println("write for "+s);
		    for(String cid : topologies.get(s).getCompo().keySet()){
		    	Component c = topologies.get(s).getCompo().get(cid);
		    	long pro = c.getLasttrans();
		    	double latency = c.getExeLatency();
//		    	double la = c.getModellatency();
		    	sen += cid+" , "+pro+", "+latency+" \n";
		    	
//		    	System.out.println("component thread size "+c.getThreads().size());
		    	for(ComponentThread ct : c.getThreads().values()){
//		    		System.out.println("updaet performance for "+cid+" thread "+ ct.getId());
		    		
		    		String hostport = ct.getExecutor().getIndex();
		    		double threadlatency = ct.getExecutelatency();
		    		long threadthr = ct.getExecute();
		    		sen += hostport+" , "+threadthr + " , "+threadlatency+"\n";
		    	}
		    sen += "\n";
		    
		    
		}
//		System.out.println("metric update update to file "+sen);
		
//		    System.out.println("set metrics "+ thr+ " , "+ lat);
//		    System.out.println("metric of "+s+" , "+metrics.get(s).latency+" , "+metrics.get(s).throughput);
		}
		System.out.println("sentence is "+sen);
		Methods.writeFile(sen, "metrics.txt");
	}
}
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

