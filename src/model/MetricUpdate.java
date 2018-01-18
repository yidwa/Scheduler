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
			Throughput thr = new Throughput(t.getTid(), t.getCompostruct(), t.getCompo());
			Latency lc = new Latency(t.getTid(), t.getCompostruct(),t.getCompo(), new ArrayList<Double>(), new ArrayList<Double>(), t.getTworker().size());
			
			this.throughput.put(t.getTid(), thr);
			this.latency.put(t.getTid(), lc);
			this.metrics.put(t.getTid(), new Metrics(t.getTid(), 0, 0));
		}
	
		
	}
	
	  public void updateLatency(String tname, ArrayList<Double> arr, ArrayList<Double> serv, long numworker){
//		  System.out.println("tname is "+tname);
	    	latency.get(tname).updateData(arr, serv, numworker,topologies.get(tname).getSystemlatencylatest());
	    	
	    	throughput.get(tname).setInputrate(arr.get(arr.size()-1));
	    }
		
//	    public void updatethroughput(String tname, HashMap<String, ArrayList<Double>> cp, HashMap<String, ArrayList<Double>> ce,HashMap<String, Long> lp, HashMap<String, Long> lc){
//	    	throughput.get(tname).updateData(cp, ce, lp, lc);
//	    }
	    
	
/**
 * ???
 */
		public void performanceMetric(){
		
			double lat;
			double thrrat = -0.1;
			String sen = "";
			
			
			for(String s: topologies.keySet()){
				
				double failrate = topologies.get(s).getFailrate();
			
				if(failrate !=0)
					thrrat = 1-failrate;
//				thrrat = throughput.get(s).throughputRatio();
				lat = latency.get(s).latestlatency;
				
				Metrics m = metrics.get(s);
			    m.setMetrics(thrrat, lat);
			    
			    sen +=  s+ " , "+ thrrat + " , "+ lat+ " \n";
			    
			    System.out.println("performance metric "+ s+" thrratio and lat are "+thrrat+" , "+lat);
			    // need to change the metric for component
			    // including the output/input, and the execute latency
			    for(String cid : topologies.get(s).getCompo().keySet()){
			    	Component c = topologies.get(s).getCompo().get(cid);
//			    	double pro = c.getTotalprocess();
//			    	double la = c.getModellatency();
			    	long emit = c.getLastemit();
			    	long ack = c.getLastack();
			    	double ratio = Double.valueOf(Methods.formatter.format(ack*1.0/emit));
			    	
			    	double compolat = 0;
			    	if (c.spout)
			    		compolat =c.getTotalprocess();
			    	else
			    	    compolat = c.getExeLatency();
			    	// la is the execute latency
			    	sen += cid+" , "+ratio+", "+compolat+" \n";
//			    	System.out.println("cid "+cid+ " , "+ratio+" , "+compolat);
//			    	if(c.spout == false)
//   			    	  sen += "sys "+topologies.get(s).getSystemcolatency().get(cid)+" \n";
			    	for(String ctid : c.threads.keySet()){
			    		sen += ctid+" ; ";
			    	}
			    	sen += "\n";
//			    		String temp = c.getThreads().get(ctid).changedtype;
//			    		if(!(temp == "")){
//			    			topologies.get(s).getChangedschedule().put(ctid, temp);
//				    		}
//			    		}
			   	}
//			    sen += topologies.get(s).getChangedschedule().toString()+" priority "+ priority.get(s)+"\n";
			    sen += "current scheduler is "+topologies.get(s).getCschedule().toString();
			    
			    sen += "\n";
			    
			    
			}
			
			
			// optimization for the model based scheuler
//			HashMap<String, String> mapping = Optimisation.cpuscheduelr(topologies, priority);
//			
//		
//			String mappresult = "";
//
//			for(String maps: mapping.keySet()){
//				mappresult += maps+"\n";
//				mappresult += mapping.get(maps).toString();
//				mappresult += "\n";
//			}
//	        
//			Methods.writeFile(mappresult, "model_schedule",false);
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
