package opt;

import java.util.ArrayList;
import java.util.HashMap;

import general.Component;
import general.Methods;
import general.PriorityQueue;
import general.Topology;

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
	
	
	
	public QoS_Opt() {
		// TODO Auto-generated constructor stub
		QoS_Opt.lowLat = 100.0;
		QoS_Opt.medLat = 50.0;
		QoS_Opt.higLat = 15.0;
		QoS_Opt.rho = 0.6;
		QoS_Opt.eta = 0.2;
		QoS_Opt.ipu = 0.2;
	
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
		dif = latency - exp;
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
//		System.out.println("inside switch cost "+hori.toString()+" , "+upd.toString());
		int addingcost = 0;
		int removingcost = 0;
		int cost = 5;
		//exist only in ori
		ArrayList<String> bu = new ArrayList<String>();
		//exist both in ori and upd
		ArrayList<String> bo = new ArrayList<String>();
		
		for(String s: hori){
			if(!upd.contains(s))
				bu.add(s);
			else 
				bo.add(s);
		}
		
//		System.out.println("bu "+bu.toString()+" , bo "+bo.toString());
		for(String s: upd){
			if(!bo.contains(s))
				addingcost += cost;
		}
		
		removingcost = bu.size() * cost;
//		System.out.println("adding cost "+ addingcost + " ,removing cost "+removingcost);
//		System.out.println("ipu "+ipu);
//		System.out.println("total cost for switching "+ ipu*(removingcost+addingcost));
		return Double.valueOf(Methods.formatter.format(ipu*(removingcost+addingcost)));
	}
	
	public static double cpuCost(ArrayList<String> hosts){
		int result = 0;
		result = getCpu(hosts);
		return Double.valueOf(Methods.formatter.format(eta*result));
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
	public static double costE(PriorityQueue pq, ArrayList<String> prohost, HashMap<String, Topology> topologies){
		new QoS_Opt();
		double result = 0;
		ArrayList<String> host = pq.getHosts();
		double switchcost = switchCost(host, prohost);
		double cpucost = cpuCost(prohost);
		double qoscost = qosQueuecost(pq, topologies, prohost.size());
		result =  switchcost + cpucost + qoscost;
		System.out.println("schedule "+prohost.toString()+" switch cost  "+switchcost+" , cpu cost "+cpucost+" , qos cost "+qoscost);
		return result;
	}
	
	public static ArrayList<String> optimizedSolution(PriorityQueue pq, HashMap<String, Topology> topologies){
		System.out.println("optimized solution for queue "+pq.getPrioirty());
		ArrayList<String> result = new ArrayList<>();
		ArrayList<ArrayList<String>> list = possibleHost(pq.getPrioirty());
		double cost = Double.MAX_VALUE;
		for (ArrayList<String> option : list){
			double tempcost = costE(pq, option, topologies);
			if (tempcost < cost){
				cost  = tempcost;
				result = option;
			}
		}
		System.out.println("the optimial cost is "+cost+ " with schedule "+result.toString());
		return result;
	}
	
	public static double qosQueuecost(PriorityQueue pq, HashMap<String, Topology> topologies, int size){
		double result = 0;
		//punishment for each topology
		int costpertopology = 10;
		ArrayList<String> tname = pq.getNames();
		//loop for all the topology that calculate the exectuion time of all components
		for(String s : tname){
			double totalexel = 0;
			Topology t = topologies.get(s);
			for(Component c : t.getCompo().values()){
				totalexel += c.getExeLatency();
			}
//			System.out.println("total execute latency for "+s+ " is "+totalexel);
			double lat = totalexel + pq.getAvgbuf() + pq.getQl().waittimeEstimating(size);
//			System.out.println("waiting time estimation is "+ pq.getQl().waittimeEstimating(size));
//			System.out.println("total latency for queue "+pq.getPrioirty()+" is "+lat);
//			System.out.println("queue latency cost is "+qosCost(lat, pq.getPrioirty()));
			result += costpertopology * qosCost(lat, pq.getPrioirty());
		}
		result = result * rho;
		return Double.valueOf(Methods.formatter.format(result));
	}
	
	
	public static ArrayList<ArrayList<String>> possibleHost(int pri){
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
