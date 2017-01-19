package general;

import java.awt.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Threads;

import model.Latency;
import model.MetricUpdate;
import model.Metrics;
import model.Throughput;

public class CentralControl {

	HashMap<String, LinkedList<Double>> arr; 
	HashMap<String, LinkedList<Double>> ser;
	HashMap<String, Metrics> metrics;
		
	public CentralControl(){
		this.arr = new HashMap<String, LinkedList<Double>>();
		this.ser = new HashMap<String, LinkedList<Double>>();
		metrics = new HashMap<String, Metrics>();
	}
	
	public static void main(String[] args) throws InterruptedException {
		
		
		CentralControl cc = new CentralControl();
//		System.out.println("main test 1");
		StormCluster sc = new StormCluster();
//		System.out.println("main test 2");
		HashMap<String, Topology> topologies = sc.topologies;
		
//		for(Topology t: topologies.values()){
//			t.printTopology();
//		}
	
		ScheduledExecutorService scheduledPooldata = Executors.newScheduledThreadPool(10);
		//model metric pool
//		ScheduledExecutorService scheduledPoolmetric = Executors.newScheduledThreadPool(10);
		for (int i = 0; i<12; i++){
			
			dataretrival dt = new dataretrival(topologies, cc.arr, cc.ser,sc.sr);
		
			scheduledPooldata.schedule(dt, 5, TimeUnit.SECONDS);
//			String temp ="";
//			if(i == 9){
//				for(String s : dt.arr.keySet()){
//					temp += dt.arr.get(s);
//					temp += ",";
////					System.out.println(s+ " arr "+temp);
//					Methods.writeFile(s+" arrival rate " + temp, "Records.txt");
//					temp = "";
//					temp += dt.ser.get(s);
//					temp += ",";
////					System.out.println(s+ " serv "+ temp);
//					Methods.writeFile(s+ " service rate " +temp, "Records.txt");
//					temp = "";
//				}
//			}
//			Thread.sleep(20*1000);	
			
			
			/*
			 * this is used for model evaluation and later scheduling purpose
			 */
//			MetricUpdate mu = new MetricUpdate(sc.sr, topologies);
//			for(String s: topologies.keySet()){
//				System.out.println("central controla update for topology "+s);
//				
//				if(cc.arr.containsKey(s)){
////					System.out.println("udpate metrics for "+s);
////				System.out.println("topolgoies size "+topologies.size()+" arr size "+cc.arr.keySet()+" ser size "+cc.ser.keySet());
////				System.out.println("metric update for "+s);
//					ArrayList<Double> arrtemp = new ArrayList<Double>(cc.arr.get(s));
//					ArrayList<Double> servtemp = new ArrayList<Double>(cc.ser.get(s));
//					mu.updateLatency(s, arrtemp, servtemp, topologies.get(s).workers);	}	
//				else{
//						System.out.println("the "+s+" is not included in the current records");
//					}
//				}
//			scheduledPoolmetric.schedule(mu, 0, TimeUnit.SECONDS);
			
//			Thread.sleep(60*2*1000);	
//			
			Thread.sleep(5*60*1000);	
			
//			Thread.sleep(10*1000);
//			String sen = "";
//			for(String s: mu.getMetrics().keySet()){
//			  sen += s+ mu.getMetrics().toString()+",";
//			}
//			Methods.writeFile(sen, "metrics.txt");
			
		}
			
		Threads.sleep(10000);
		
		scheduledPooldata.shutdown();
//		scheduledPoolmetric.shutdown();
//		
//		while(!scheduledPooldata.isTerminated() && !scheduledPoolmetric.isTerminated()){
//			}
	
//		
		while(!scheduledPooldata.isTerminated()){
		}
		
		
		System.out.println("all finished");
	}
}