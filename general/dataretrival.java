package general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Threads;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class dataretrival {

//	HashMap<String, String> topo;
//	
//	public dataretrival(){
//		this.topo = new HashMap<>();
//	}
//	public static void createJson(){
//		
//		JSONObject obj = new JSONObject();
//		obj.put("name", "yidan");
//		obj.put("age", new Integer(28));
//		
//		JSONArray list = new JSONArray();
//		list.add("msg 1");
//		list.add("msg 2");
//		list.add("msg 3");
//		
//		obj.put("message", list);
//		
//		try{
//			
//			FileWriter file = new FileWriter(new File("/Users/yidwa/Desktop/testing.json"));
//			file.write(obj.toJSONString());
//			file.flush();
//			file.close();
//		}
//		catch (IOException e){
//			e.printStackTrace();
//		}
//		
//		System.out.println(obj);
//		
//	}
	
//	public static void readJson(){
//		JSONParser parser = new JSONParser();
//		
//		try{
//			Object obj = parser.parse(new FileReader("/Users/yidwa/Desktop/testing.json"));
//			
//			JSONObject jo = (JSONObject) obj;
//			
//			String name = (String) jo.get("name");
//			System.out.println(name);
//			
//			long age = (Long) jo.get("age");
//			System.out.println(age);
//			
//			JSONArray msg = (JSONArray) jo.get("message");
//			Iterator<String> it = msg.iterator();
//			while(it.hasNext()){
//				System.out.println(it.next());
//			}
//			
//		}
//		catch (FileNotFoundException e){
//			e.printStackTrace();
//		}
//		catch (IOException e){
//			e.printStackTrace();
//		}
//		catch (ParseException e){
//			e.printStackTrace();
//		}
//	}
	
//	public static ArrayList<Supervisor> getworkers(HashMap<String, Supervisor> workers){
//		ArrayList<Supervisor> w = new ArrayList<Supervisor>();
//		for (Map.Entry<String, Supervisor> entry : workers.entrySet()){
//			w.add(entry.getValue());
//		}
//		return w;
//	}
	
	public static void main(String[] args) throws InterruptedException, IOException {
	
		StormREST sr = new StormREST("http://115.146.85.187:8080");

	//	String freq = args[0];
	
		sr.Topologyget();
		

		sr.Supervisorinfo();
		sr.Topologyinfo();
//	
		sr.topologySum();
		
		
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
	ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(5);
////
////	
	for (int i = 0; i< 14; i++){
//		
		PerformanceUpdate pu = new PerformanceUpdate(sr.topologies, sr.hostport);
////		SuperVisorUpdate update = new SuperVisorUpdate(sr.workers, sr.hostport);
		scheduledPool.schedule(pu, 0, TimeUnit.SECONDS);
////		scheduledPool.scheduleAtFixedRate(pu, 0, 20, TimeUnit.SECONDS);
		System.out.println("new thread start");
		Thread.sleep(10*60*1000);
	}

	
////		
	Threads.sleep(3000);
	
	scheduledPool.shutdown();
	
	while(!scheduledPool.isTerminated()){
		}
	
	System.out.println("all finished");

	}
//	}
}

