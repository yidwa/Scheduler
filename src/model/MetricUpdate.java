package model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import general.Component;
import general.Executor;
import general.Methods;
import general.StormREST;
import general.Topology;

public class MetricUpdate implements Runnable {
	
	HashMap<String, Topology> topologies;
	HashMap<String, Throughput> throughput;
	HashMap<String, Latency> latency;
	HashMap<String, Metrics> metrics;
	
	public HashMap<String, Metrics> getMetrics() {
		return metrics;
	}

	public void setMetrics(HashMap<String, Metrics> metrics) {
		this.metrics = metrics;
	}

	public MetricUpdate(StormREST sr,HashMap<String, Topology>  topologies) {
		// TODO Auto-generated constructor stub
		this.topologies = topologies;
		this.throughput = new HashMap<String,Throughput>();
		this.latency = new HashMap<String,Latency>();
		this.metrics = new HashMap<String, Metrics>();
	
		for(Topology t: topologies.values()){
			Throughput thr = new Throughput(t.getTid(), t.getCompostruct(), t.getCompo());
			Latency lc = new Latency(t.getTid(), t.getCompostruct(),t.getCompo(), new ArrayList<Double>(), new ArrayList<Double>(), t.getTworker().size());
			
			this.throughput.put(t.getTid(), thr);
			this.latency.put(t.getTid(), lc);
			this.metrics.put(t.getTid(), new Metrics(t.getTid(), 0, 0));
		}
		
	}
	
	  public void updateLatency(String tname, ArrayList<Double> arr, ArrayList<Double> serv, long numworker){
//		  System.out.println("tname is "+tname);
	    	latency.get(tname).updateData(arr, serv, numworker);
	    	
	    	throughput.get(tname).setInputrate(arr.get(arr.size()-1));
	    }
		
//	    public void updatethroughput(String tname, HashMap<String, ArrayList<Double>> cp, HashMap<String, ArrayList<Double>> ce,HashMap<String, Long> lp, HashMap<String, Long> lc){
//	    	throughput.get(tname).updateData(cp, ce, lp, lc);
//	    }
	    
	

		public void performanceMetric(){
			long thr;
			double lat;
			String sen = "";
			for(String s: topologies.keySet()){
				thr = throughput.get(s).totalThroughput();
//				System.out.println("througput in performance metric "+thr);
				//add the spouting tuples
				thr += throughput.get(s).getInputrate();
//				System.out.println("throughput after adding input rate "+thr);
				lat = latency.get(s).totallatency();
//				System.out.println("perofmrance update "+s+" , "+thr +" , "+lat);
				Metrics m = metrics.get(s);
			    m.setMetrics(thr, lat);
			    sen += s+" model  , + thr "+thr+" , lat "+lat+"\n";
			    sen += "system states "+ topologies.get(s).getSystememit()+" , "+topologies.get(s).getSystemlatency()+"\n";
			    for(String cid : topologies.get(s).getCompo().keySet()){
			    	Component c = topologies.get(s).getCompo().get(cid);
			    	double pro = c.getTotalprocess();
			    	double la = c.getModellatency();
			    	sen += "model "+cid+" , "+pro+", "+la+" \n";
			    	if(c.spout == false)
   			    	  sen += "sys "+topologies.get(s).getSystemcolatency().get(cid)+" \n";
			    	}
			    sen += "\n";
			    
			    
			}
			System.out.println("metric update update to file "+sen);
			Methods.writeFile(sen, "metrics.txt");
//			    System.out.println("set metrics "+ thr+ " , "+ lat);
//			    System.out.println("metric of "+s+" , "+metrics.get(s).latency+" , "+metrics.get(s).throughput);
			}
			   
			
	
//		public void updateData(){
//			
//		}
	@Override
	public void run() {
//		System.out.println("no metric update ");
//		// TODO Auto-generated method stub
		System.out.println("new Metricupdate starts");
		performanceMetric();
	}
	
	
	
	public HashMap<String, Topology> getTopologies() {
		return topologies;
	}

	public void setTopologies(HashMap<String, Topology> topologies) {
		this.topologies = topologies;
	}

	public HashMap<String, Throughput> getThroughput() {
		return throughput;
	}

	public void setThroughput(HashMap<String, Throughput> throughput) {
		this.throughput = throughput;
	}

	public HashMap<String, Latency> getLatency() {
		return latency;
	}

	public void setLatency(HashMap<String, Latency> latency) {
		this.latency = latency;
	}
}
