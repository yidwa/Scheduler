package opt;

import java.util.ArrayList;

import general.PriorityQueue;

public class QoS_Opt {

	// expected latencies
	double lowLat;
	double medLat;
	double higLat;
	// factor for qos, cpu and swtich cost
	double rho;
	double eta;
	double ipu;
	// twitter 50 ms , line 15 ms, diamond 20s, star 18ms
	
	
	
	public QoS_Opt() {
		// TODO Auto-generated constructor stub
		this.lowLat = 100.0;
		this.medLat = 50.0;
		this.higLat = 15.0;
		this.rho = 0.4;
		this.eta = 0.3;
		this.ipu = 0.3;
	
	}
	
	public double getExp(int pri){
		if (pri == 1)
			return higLat;
		else if (pri == 3)
			return lowLat;
		else
			return medLat;
	}
	
	public int getCpu(ArrayList<String> hosts){
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
	 * calculate the violations for each queue
	 * @param latency
	 * @param pri
	 * @return
	 */
	public double qosCost(double latency, int pri){
		double dif;
		double exp  = getExp(pri);
		dif = latency - exp;
		if(dif <= 0)
			return 0;
		else 
			return rho*dif;
	}
	/**
	 * calculate the cost for switching, including the add/remove vm or change the host, comparing the proposed list with original one
	 * @return
	 */
	public double switchCost(ArrayList<String> hori, ArrayList<String> upd){
	
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
		
		for(String s: upd){
			if(!bo.contains(s))
				addingcost += cost;
		}
		
		removingcost = bu.size() * cost;
		
		return ipu*(removingcost+addingcost);
	}
	
	public double cpuCost(ArrayList<String> hosts){
		int result = 0;
		result = getCpu(hosts);
		return eta*result;
	}
	
	
//	/**
//	 * calculate the cost for queue pq by using the generated host list
//	 * @param pq
//	 * @param prohost, random one
//	 * @return
//	 */
//	public double costE(PriorityQueue pq, ArrayList<String> prohost){
//		double result = 0;
//		ArrayList<String> host = pq.getHosts();
//		double switchcost = switchCost(host, prohost);
//		double cpucost = cpuCost(prohost);
//		double lat ;
//	
//		return result;
//		
//	}
	
	
	public static void main(String[] args) {
		QoS_Opt qo = new QoS_Opt();
		ArrayList<String> ori = new ArrayList<>();
		ArrayList<String> upd = new ArrayList<>();
		ori.add("h1");
		ori.add("h2");
//		ori.add("h3");
		upd.add("h1");
		upd.add("h3");
		System.out.println(qo.switchCost(ori, upd));
	}
}
