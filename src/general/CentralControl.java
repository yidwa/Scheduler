package general;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.storm.starter.ThroughputVsLatency.C;

import model.MetricUpdate;
//import model.MetricUpdate;
import model.Metrics;
import model.QueueUpdate;

public class CentralControl {

	HashMap<String, LinkedList<Double>> arr; 
	HashMap<String, LinkedList<Double>> ser;
	HashMap<String, Metrics> metrics;
	HashMap<String, Topology> topologies;
	static HashMap<String, Integer> priority;
	ArrayList<PriorityQueue> queues;
	
	public CentralControl(){
		this.arr = new HashMap<String, LinkedList<Double>>();
		this.ser = new HashMap<String, LinkedList<Double>>();
		this.metrics = new HashMap<String, Metrics>();
		queues = new ArrayList<PriorityQueue>();
	}
	
	/**
	 * the overall production and testing parameters control
	 * 5,60 for tests and 12,300 for the production
	 * @param loop,  the time for run
	 * @param waitime, the time wait time between each iteration
	 * @throws InterruptedException
	 */
	public static void executeParameter(int loop, int waitime) throws InterruptedException{
		ScheduledExecutorService scheduledPooldata = Executors.newScheduledThreadPool(10); 
		ScheduledExecutorService scheduledPoolmetric = Executors.newScheduledThreadPool(10);
		CentralControl cc = new CentralControl();
		StormCluster sc = new StormCluster();
		cc.topologies = sc.topologies;
		priority = sc.priority; 
		cc.queues = sc.queue;
		for (int i = 0; i<loop; i++){
			dataretrival dt = new dataretrival(cc.topologies, getPriority(), cc.arr, cc.ser,sc.sr, cc.queues);
			scheduledPooldata.schedule(dt, 5, TimeUnit.SECONDS);
			Thread.sleep(20*1000);
			
			
//			for QoS scheduling	of latency 
//			QueueUpdate qu = new QueueUpdate(sc.sr, cc.topologies, priority, cc.queues, cc.metrics, cc.arr, cc.ser, true, 0.4, 0.3, 0.3);
//			for QoS scheduling	of throughput 
			QueueUpdate qu = new QueueUpdate(sc.sr, cc.topologies, priority, cc.queues, cc.arr, cc.ser, false, 0.4, 0.3, 0.3);
			
			for(PriorityQueue pq : cc.queues){
			   if(pq.size>0){
				   qu.updateLatency(cc.queues, pq.getPrioirty(), pq.getArr(), pq.getServ());
			   	}
			   }
			
			scheduledPoolmetric.schedule(qu, 0, TimeUnit.SECONDS);
			
			Thread.sleep(waitime*1000);
			
		
//			System.out.println("topology size now is "+cc.topologies.size());
//			for(String s:cc.topologies.keySet()){
//				System.out.println("topology "+ s+" , contains compoennt ");
//				for(String cid : cc.topologies.get(s).getCompo().keySet()){
//					System.out.println(cid+" , ");
//					System.out.println("with ack "+ cc.topologies.get(s).getCompo().get(cid).getLastack()+" , and emit "+ cc.topologies.get(s).getCompo().get(cid).getLastemit());
//				}
//			}
			// for cpu scheduler	
						//for topology based scheduling
			MetricUpdate mu = new MetricUpdate(sc.sr, cc.topologies, priority);
			for(String s: cc.topologies.keySet()){
				if(cc.arr.containsKey(s)){
					ArrayList<Double> arrtemp = new ArrayList<Double>(cc.arr.get(s));
					ArrayList<Double> servtemp = new ArrayList<Double>(cc.ser.get(s));
					mu.updateLatency(s, arrtemp, servtemp, cc.topologies.get(s).workers);	
					
					System.out.println("metric update for  "+s+" with arr "+arrtemp.get(arrtemp.size()-1)+" , serv "+servtemp.get(servtemp.size()-1));
				}	
				else{
					System.out.println("the "+s+" is not included in the current records");
				}
			}
			scheduledPoolmetric.schedule(mu, 0, TimeUnit.SECONDS);
			Thread.sleep(20*1000);
			
		}
			
		Threads.sleep(10000);
		
		scheduledPooldata.shutdown();
		scheduledPoolmetric.shutdown();
		while(!scheduledPooldata.isTerminated() && !scheduledPoolmetric.isTerminated()){
			}
	}
	
	public static HashMap<String, Integer> getPriority() {
		return priority;
	}

	public static void setPriority(HashMap<String, Integer> priority) {
		CentralControl.priority = priority;
	}

	public static void main(String[] args) throws InterruptedException {
		int loop = 3;
//		int loop = 30;
		int waitime = 30;
//		int waitime =120;
		CentralControl.executeParameter(loop, waitime);
		System.out.println("all finished");
	}
}