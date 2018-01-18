package opt;

import java.util.ArrayList;
import java.util.HashMap;

import com.yammer.metrics.core.Metric;

import general.Component;
import general.Methods;
import general.PriorityQueue;
import general.Topology;
import model.Metrics;

public class QoS_Opt {

	// expected latencies
	static double lowLat;
	static double medLat;
	static double higLat;
	// factor for qos, cpu and swtich cost
	static double rho;
	static double eta;
	static double ipu;
	// twitter 50 ms , line 15 ms, diamond 20s, star 18ms
	static double lowThr;
	static double medThr;
	static double higThr;
	
/**
 * initialize the parameters, by default 0.6, 0.2, 0.2
 * @param lat true if the qos is latency related, false if the qos is throughput related
 */
	public QoS_Opt(boolean lat, double qos, double cpu, double switchc) {
		// TODO Auto-generated constructor stub
		if(lat){
			QoS_Opt.lowLat = 55.0;
			QoS_Opt.medLat = 25.0;
			QoS_Opt.higLat = 15.0;
		}
		else{
			QoS_Opt.lowThr = 0.80;
			QoS_Opt.medThr = 0.90;
			QoS_Opt.higThr = 0.95;
		}
		QoS_Opt.rho = qos;
		QoS_Opt.eta = cpu;
		QoS_Opt.ipu = switchc;

	}
	
	public static double getExp(int pri){
		if (pri == 1)
			return higLat;
		else if (pri == 3)
			return lowLat;
		else
			return medLat;
	}
	
	public static int getCpu(ArrayList<String> hosts){
		int result = 0;
		for(String s : hosts){
			if(s.contains("l"))
				result += 12;
			else if(s.contains("m"))
				result += 8;
			else if(s.contains("s"))
				result += 4; 
		}
		return result;
	}
	/**
	 * calculate the violations for each queue by the given estimated latency
	 * @param latency
	 * @param pri
	 * @return
	 */
	public static double qosCost(double latency, int pri){
		double dif;
		double exp  = getExp(pri);
//		dif = latency - exp;
		if(exp!=0)
			dif = latency - exp/exp;
		else
			dif = latency-exp;
		if(dif <= 0)
			return 0;
		else 
			return Double.valueOf(Methods.formatter.format(rho*dif));
	}
	/**
	 * calculate the cost for switching, including the add/remove vm or change the host, comparing the proposed list with original one
	 * @return
	 */
	public static double switchCost(ArrayList<String> hori, ArrayList<String> upd){
		int addingcost = 0;
		int removingcost = 0;
		int cost = 8;
		//remove list
		ArrayList<String> bu = new ArrayList<String>();
		//no changel list
		ArrayList<String> bo = new ArrayList<String>();
		
		for(String s: hori){
			if(!upd.contains(s))
				bu.add(s);
			else 
				bo.add(s);
		}
		
		for(String s: upd){
			if(!bo.contains(s))
				addingcost += cost;
		}
		
		removingcost = bu.size() * cost;
		return removingcost+addingcost;
	}
	
	

	public static double cpuCost(ArrayList<String> hosts){
		int result = 0;
		result = getCpu(hosts);
		return result;
	}
	
	/**
	 * 
	 * @param orilatency
	 * @param orihosts
	 * @param prohosts
	 * @param buf
	 * @param wait
	 * @return
	 */
	public static double possibleLatency(double orilatency, ArrayList<String> orihosts, ArrayList<String> prohosts, double buf, double wait){
		double result = 0;
		double exelat = orilatency - wait - buf;
		int c1 = getCpu(orihosts);
		int c2 = getCpu(prohosts);
		exelat = exelat * (c1/c2);
		double updatewait = wait * (prohosts.size()/orihosts.size());
		result = exelat + buf + updatewait;
		return result;
	}
	/**
	 * calculate the cost for queue pq by using the generated host list
	 * @param pq
	 * @param prohost, random one
	 * @return
	 */
	public static double costE(PriorityQueue pq, ArrayList<String> prohost, HashMap<String, Topology> topologies, boolean latqos, double qos,
			double cpu, double swi){
		new QoS_Opt(latqos, qos, cpu, swi);
		double result = 0;
		ArrayList<String> host = pq.getHosts();
		double switchcost = switchCost(host, prohost);
		double cpucost = cpuCost(prohost);
		double qoscost = qosQueuecost(pq, topologies, host, prohost, latqos, qos);
		result =  switchcost * swi+ cpucost * cpu + qoscost * qos;
		System.out.println("now  "+host+" , proposed schedule "+prohost.toString()+" costs  are "+switchcost + " , "+ cpucost+ " , "+qoscost);
		result = Double.valueOf(Methods.formatter.format(result));
		return result;
	}
	
	/**
	 * return the option with least cost by iterating all options
	 * @param pq
	 * @param topologies
	 * @return
	 */
	public static ArrayList<String> optimizedSolution(PriorityQueue pq, HashMap<String, Topology> topologies, boolean latqos, double qos, double cpu, double swi){
		ArrayList<String> result = new ArrayList<>();
		// contains all possible combinations of the hosts
		ArrayList<ArrayList<String>> list = possibleHostL(pq.getPrioirty());
		double cost = Double.MAX_VALUE;
		for (ArrayList<String> option : list){
			// estimate the cost for the gvien option
			double tempcost = costE(pq, option, topologies, latqos, qos, cpu, swi);
			if (tempcost < cost){
				cost  = tempcost;
				result = option;
			}
		}
		return result;
	}
	
	/*
	 * calculate the cost for QoS violation, in respect to priority , while it concerns both throughput and latency
	 * for latency, should estimate the service time together with the buffer time, to evaluate the qos violations
	 */
	public static double qosQueuecost(PriorityQueue pq, HashMap<String, Topology> topologies, ArrayList<String> host, ArrayList<String> prohost, boolean qoslat, 
			double qosweight){
		double result = 0;
		double stanl = 0;
		double stant = 0;
		//punishment for each topology
		double penalty;
		if (pq.getPrioirty()==1 ){
			penalty = 1.5;
			stanl = higLat;
			stant = higThr;
		}
		else if (pq.getPrioirty() ==2){
			penalty = 1.2;
			stanl = medLat;
			stant = medThr;
		}
		else{
			penalty = 1;
			stanl = lowLat;
			stant = lowThr;
		}
//		ArrayList<String> tname = pq.getNames();
		
		
		// the qos cost should be considered as latency violation
		if(qoslat){
//			double exeest = pq.getQl().exetimeEstimation(host, prohost);
			int ori = getCpu(host);
			int aft = getCpu(prohost);
			double exelat = pq.getQl().meanserv;
			double estlat;
			if(aft != 0 )
				 estlat = ori*exelat/aft;
			else 
				estlat = 0;
			
			double lat = estlat + pq.getAvgbuf();
			
			lat = Double.valueOf(Methods.formatter.format(lat));
			
//			System.out.println("inside qos cost, lat ="+lat+ "ori, after "+ori+ " , "+aft+" , execute and estimate latency are "+exelat+" , "+ estlat+" avg buff "
//					+ pq.getAvgbuf());
			
			double dif = lat - stanl;
			
//			System.out.println("dif "+dif);
			if(dif < 0)
				return 0;
			else
				result = dif * penalty;
		}
		// the qos cost is concerning throughput
		else{
			// need to udpate
			    System.out.println();
				System.out.println("qos queue cost for thorughput");
				double thr = 0;
				//valide topology has the positive number of throughput ratio
				int count = 0;
		
				for(String s : pq.getNames()){
					double fr = topologies.get(s).getFailrate();
				    double t = 1-fr;
					if(t!=0){
						thr += t;
						count+=1;
					}
					
				}
				System.out.println("thr "+thr+" , count "+count);
		

				double avgthr = 0;
				if(count!=0)
					avgthr = thr*1.0/count;
	
				System.out.println("the avg throughput ratio for "+ pq.getPrioirty()+ "is "+avgthr);
//
				double dif = avgthr - stant;
				System.out.println("dif is "+dif);

				if(dif>0)
					return 0;
				else
					result = dif*penalty;
		}
//			for(String s : tname){
//				double totalexel = 0;
//				Topology t = topologies.get(s);
//				for(Component c : t.getCompo().values()){
//					totalexel += c.getExeLatency();
//				}
//				double lat = totalexel + pq.getAvgbuf() + wait ;
//				result += costpertopology * qosCost(lat, pq.getPrioirty());
//			}
	return result;
	}
	
	/**
	 * update the list for queue with label as priority
	 * different options(for instance, p =1 , including s1, m1, l1, (s1,m1), (s1,l1), (m1,l1), (s1,m1,l1) 
	 * @param pri
	 * @return
	 */

	public static ArrayList<ArrayList<String>> possibleHostL(int pri){
		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		String s = "s"+String.valueOf(pri);
		String m = "m"+String.valueOf(pri);
		String l = "l"+String.valueOf(pri);
		
		ArrayList<String> a = new ArrayList<>();
		a.add(s);
		ArrayList<String> b = new ArrayList<>();
		b.add(m);
		ArrayList<String> c = new ArrayList<>();
		c.add(l);
		
		list.add(a);
		list.add(b);
		list.add(c);
		
		ArrayList<String> ab = new ArrayList<>();
		ab.add(s);
		ab.add(m);
		
		list.add(ab);
		
		ArrayList<String> bc = new ArrayList<>();
		bc.add(m);
		bc.add(l);
		
		list.add(bc);
		
		ArrayList<String> ac = new ArrayList<>();
		ac.add(s);
		ac.add(l);
		
		list.add(ac);
		
		ArrayList<String> abc = new ArrayList<>();
		abc.add(s);
		abc.add(m);
		abc.add(l);
		
		list.add(abc);
		
		return list;
	}
	
//	public static void main(String[] args) {
//		QoS_Opt qo = new QoS_Opt();
//		System.out.println(qo.possibleHost(2));
//		System.out.println(qo.possibleHost(3));
////		ArrayList<String> ori = new ArrayList<>();
////		ArrayList<String> upd = new ArrayList<>();
////		ori.add("l1");
////		ori.add("m2");
//////		ori.add("h3");
////		upd.add("l1");
////		upd.add("l3");
////		System.out.println(qo.cpuCost(ori));
////		System.out.println(qo.cpuCost(upd));
////		System.out.println(qo.switchCost(ori, upd));
//	}
}
