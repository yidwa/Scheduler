package model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import general.Component;
import general.Executor;
import general.Methods;
import general.StormREST;
import general.Topology;
import opt.Optimisation;


// used for cpu scheduler


public class MetricUpdate implements Runnable {
	
	HashMap<String, Topology> topologies;
	HashMap<String, Integer> priority;
	HashMap<String, Throughput> throughput;
	HashMap<String, Latency> latency;
	HashMap<String, Metrics> metrics;
	
	public HashMap<String, Metrics> getMetrics() {
		return metrics;
	}

	public void setMetrics(HashMap<String, Metrics> metrics) {
		this.metrics = metrics;
	}

	public MetricUpdate(StormREST sr,HashMap<String, Topology>  topologies, HashMap<String, Integer> priority) {
		// TODO Auto-generated constructor stub
		this.topologies = topologies;
		this.priority = priority;
		this.throughput = new HashMap<String,Throughput>();
		this.latency = new HashMap<String,Latency>();
		this.metrics = new HashMap<String, Metrics>();
		
		for(Topology t: topologies.values()){
			System.out.println("inside constructor, tid is "+t.getTid()+" . size is  "+t.getCompo().size());
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
	    
	
/**
 * ???
 */
		public void performanceMetric(){
			System.out.println("performance metric ready");
			
			long thr;
			double lat;
			double thrrat;
			String sen = "";
			for(String s: topologies.keySet()){
				
				// reserved for the model-based scheduler
//				thr = throughput.get(s).totalThroughput();
				
				
				thrrat = throughput.get(s).throughputRatio();
				System.out.println("thrarat is "+ thrrat);
				//add the spouting tuples
//				thr += throughput.get(s).getInputrate();
//				System.out.println("throughput after adding input rate "+thr);
				lat = latency.get(s).totallatency();
//				System.out.println("perofmrance update "+s+" , "+thr +" , "+lat);
				Metrics m = metrics.get(s);
//			    m.setMetrics(thr, lat);
//			    sen += s+" model  , + thr "+thr+" , lat "+lat+"\n";
			    String output = String.valueOf(throughput.get(s).getOutput());
			    Long l = throughput.get(s).tThroughput();
//			    System.out.println("output is "+output);
//			    sen += s+" model  , + thr "+thr+" ,  output "+ l +" , "+lat+"\n";
			    sen += "system states "+ topologies.get(s).getSystememit()+" , "+ l +" , "+topologies.get(s).getSystemlatency()+"\n";
			    for(String cid : topologies.get(s).getCompo().keySet()){
			    	Component c = topologies.get(s).getCompo().get(cid);
			    	double pro = c.getTotalprocess();
			    	double la = c.getModellatency();
			    	// la is the execute latency
			    	sen += "model "+cid+" , "+pro+", "+la+" \n";
			    	if(c.spout == false)
   			    	  sen += "sys "+topologies.get(s).getSystemcolatency().get(cid)+" \n";
			    	for(String ctid : c.threads.keySet()){
			    		String temp = c.getThreads().get(ctid).changedtype;
			    		if(!(temp == "")){
			    			topologies.get(s).getChangedschedule().put(ctid, temp);
				    		}
			    		}
			    	}
			    
			    sen += topologies.get(s).getChangedschedule().toString()+" priority "+ priority.get(s)+"\n";
			    sen += "current scheduler is "+topologies.get(s).getCschedule().toString();
			    sen += "\n";
			    
			    
			}
			//comment for tesitng purpose
//			System.out.println("metric update "+sen);
//			System.out.println("priority check "+priority.size());
//			System.out.println("before optimiation ");
			
			
			
			// optimization for the model based scheuler
			HashMap<String, String> mapping = Optimisation.cpuscheduelr(topologies, priority);
			
			
			
//			System.out.println("after optimiation ");
//			System.out.println("after cpuscheduler "+mapping.size());
			String mappresult = "";
//			System.out.println("mapping result ");
//			for(String s: mapping.keySet()){
//				System.out.println(mapping.get(s).toString());
//			}
			for(String s: mapping.keySet()){
				mappresult += s+"\n";
				mappresult += mapping.get(s).toString();
				mappresult += "\n";
			}
	        
			Methods.writeFile(mappresult, "model_schedule",false);
			//	System.out.println("usage in metric update "+Optimisation.cpuscheduelr(topologies, priority).toString());
			Methods.writeFile(sen, "topology_metrics.txt",true);
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
		System.out.println();
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
