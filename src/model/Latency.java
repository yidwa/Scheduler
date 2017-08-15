package model;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.crypto.Data;

import general.Component;
import general.Methods;

public class Latency {
//		public Long TimeUnit;
		// the number of tuple arrives to the system per time unit
		public ArrayList<Double> ArrivalPt;
		public ArrayList<Double> ServicePt;
		public double uti;
		// the number of working slot available
		public long numserver;
		public double meanserv;
		public double meanarr;
		public String tname;
		public ArrayList<ArrayList<String>> compostruc;
		public HashMap<String,Component> compo;
//		public HashMap<String,Component> list;
		public double inputrate;
		public int layer;
		public HashMap<String, ArrayList<Double>> compoprob;
		public HashMap<String,ArrayList<Double>> compoexe;
//		public HashMap<String,Long> compolastproc;
//		public HashMap<String,Long> compolasttran;
		public HashMap<String, ArrayList<String>> compoports;
		public String tid;
		
		public Latency(String tname, ArrayList<ArrayList<String>> compostruc, HashMap<String,Component> compo, ArrayList<Double> arr, ArrayList<Double> serv, long numserver){
			this.tname = tname;
			this.ArrivalPt = arr;
			this.ServicePt = serv;
			this.numserver = numserver;
			this.compo = compo;
			this.tid = tname;
			this.compostruc = compostruc;
//			this.list = addingcom(components);
//			this.inputrate = inputrate;
			this.layer = compostruc.size();
//			this.compoprob = new HashMap<String, HashMap<String,Double>>();
			this.compoexe = new HashMap<String, ArrayList<Double>>();
//			this.compolastproc = new HashMap<String, Long>();
//			this.compolasttran = new HashMap<String, Long>();
			this.compoports = new HashMap<String, ArrayList<String>>();
//			this.uti = getUti();
		}
		
		public double totallatency(){
			double ll = 0;
			
			double latency = 0;
			Component c;
//			System.out.println("calculate latency for "+tid);
			for(int i =1; i<layer; i++){
				int temp = compostruc.get(i).size();
//				System.out.println("latency total latency layer "+i);
				for(int j = 0; j<compostruc.get(i).size(); j++){
					String value = compostruc.get(i).get(j);
//					System.out.println("latency total latency value "+value);
					c = compo.get(value);
					String s = c.cid;
//					System.out.println("latency total latency before estimatino ");
					ll = c.waittimeEstimating();
//					System.out.println("latency total latency after estimatino ");
					temp+= ll;
				}
//				
//					System.out.println("input at layer "+(i+1)+ " is "+input);
			}
//			System.out.println("totalthorughput final result is "+l);
			
			return latency;
		}
//		public double getUti(){
//		
//			if(!ArrivalPt.isEmpty()&&!ServicePt.isEmpty()){
//				meanserv = DataCollection.Mean(ServicePt);
//				meanarr = DataCollection.Mean(ArrivalPt);
////				 System.out.println("mean serv "+ meanserv + " mean arr "+ meanarr);
//			}
//			if (meanserv == 0 || numserver == 0)
//				return 0;
//			return meanarr/(numserver*meanserv);
//			
//		}
		// can be called only after update values
//		public double waittimeEstimating(){
//			uti = getUti();
//			String temp="";
//			double pm = waitProb();
//			
//			double t1 = 0;
//			if (meanserv == 0 || uti == 1){
//				System.out.println("mean serve frequency is 0 or the util is 1");
//			}
//			t1 = pm/(meanserv*(1-uti));
////			double meanarr = DataCollection.Mean(ArrivalPt);
//			double cs = DataCollection.cv(ServicePt, meanserv);
//			double ca = DataCollection.cv(ArrivalPt, meanarr);
////			System.out.println("cs "+cs + " ca "+ca+ " cs+ca "+ (ca+cs));
//			double tt = 2*numserver;
//			double t2 = (ca+cs)/tt;
////			System.out.println("t1 is "+t1);
////			System.out.println("t2 is "+t2+ " t1* t2 = "+ t1*t2);
//		    DecimalFormat formatter = new DecimalFormat("#0.000");
////		    System.out.println(formatter.format(t1*t2));
//		    double result = Double.valueOf(formatter.format(t1*t2*1000000000));
////		    temp+= "uti " +uti+", ";
////		    temp+= "wait possibility " +pm+ ", ";
////		    temp+= "t1 "+ t1+" ,t2 "+t2+" result "+ result+"\n";
////		    Methods.writeFile(temp, "Results.txt");
////		    System.out.println("wait time estimation "+result);
//			return result;
//		}
//		
//		public double waitProb(){
//			if (uti>=0.7){
//				return ((Math.pow(uti, numserver)+uti)/2);
//			}
//			else{
//				double t = (double)(numserver+1)/2;
//				return Math.pow(uti, t);
//			}
//		}
//		
		public void updateArr(ArrayList<Double> arr){
			ArrivalPt = arr;
		}
		public void updateServ(ArrayList<Double> serv){
		    ServicePt = serv;
		}
		
	   public void updateSlotnum(long num){
		   numserver = num;
	   }
	   
	   public void updateData(ArrayList<Double> arr, ArrayList<Double> serv, long num){

		   updateArr(arr);
		   updateServ(serv);
		   updateSlotnum(num);
	   }
//		public static void main(String[] args) {
//			ArrayList<Double> test1 = new ArrayList<Double>();
//			test1.add(2.0);
//			test1.add(3.0);
//			test1.add(4.0);
//			ArrayList<Double> test2 = new ArrayList<Double>();
//			test2.add(4.0);
//			test2.add(5.0);
//			test2.add(6.0);
//			Latency lc = new Latency(test1, test2, 2);
////			double ca = DataCollection.cv(lc.ArrivalPt, lc.meanarr);
////			double cs = DataCollection.cv(lc.ServicePt, lc.meanserv);
//			
////			System.out.println((ca+cs)/4);
////			System.out.println("result "+Math.pow(0.3, 1.5)/(lc.meanserv*(1-lc.uti)));
////			System.out.println(lc.waitProb(lc.uti, 2));
//			System.out.println(lc.waittimeEstimating());
////			for(Double d : test){
////				k+= d*d;
////			}
////			System.out.println(k/test.size()/25-1);
////			System.out.println(DataCollection.cv(test, mean));
//		}
		
}
