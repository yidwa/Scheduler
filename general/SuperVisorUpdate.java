package general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class SuperVisorUpdate implements Runnable{
	HashMap<String,Supervisor> workers;
 //	Supervisor s;
	double usedCpu;
	double usedMem;
	int usedSlot;
	URL url;
	String hostport;
	HttpURLConnection conn;
	Object obj;
	JSONObject jobj;
	File sysinfo;
	FileWriter fw;
	
	
	public SuperVisorUpdate(HashMap<String, Supervisor> workers, String hostport) throws IOException {
		// TODO Auto-generated constructor stub
		this.workers = workers;
		url = new URL(hostport+"/api/v1/supervisor/summary");
		conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		if (conn.getResponseCode() != 200){
			throw new RuntimeException("Failed : http error code"+ conn.getResponseCode());
			}
		
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub

			try {
				
				BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				String output;
		
				
				while((output = br.readLine()) != null){
					JSONParser parser = new JSONParser();
				//	System.out.println(output);
					obj = parser.parse(output);
					jobj = (JSONObject)obj;
					JSONArray topo = (JSONArray) jobj.get("supervisors");
//					System.out.println("size is "+topo.size());
					for (int i = 0 ; i< topo.size(); i++){
						Object tobj = topo.get(i);
						JSONObject tjobj = (JSONObject) tobj;
				//		System.out.println(tjobj.get("id"));
						String host = (String)tjobj.get("host");
						double uptCpu = (Double)tjobj.get("usedCpu");
						double uptMem = (Double)tjobj.get("usedMem");
						long uptSlot = (Long)tjobj.get("slotsUsed");
						Supervisor t = workers.get(host);
						t.updatecpu(uptCpu);
						t.updatemem(uptMem);
						t.setUsedCPU(uptCpu);
						t.setUsedMem(uptMem);
						t.setUsedslot(uptSlot);
					
//						Supervisor s = new Supervisor((String)tjobj.get("id"),(Long)tjobj.get("slotsTotal"),
//								(Long)tjobj.get("slotsUsed"),(Double)tjobj.get("totalMem"),(Double)tjobj.get("totalCpu"),
//								(Double)tjobj.get("usedMem"),(Double)tjobj.get("usedCpu"));
////						workers.add(new Supervisor((String)tjobj.get("id"),(String)tjobj.get("host"),(Long)tjobj.get("slotsTotal"),
////								(Long)tjobj.get("slotsUsed"),(Double)tjobj.get("totalMem"),(Double)tjobj.get("totalCpu"),
////								(Double)tjobj.get("usedMem"),(Double)tjobj.get("usedCpu")));
//						workers.put(host, s);
					}
				//	System.out.println("worker size "+ workers.size());
					Thread t = new Thread(new SVfileupdate(workers));
					t.start();
			
					//System.out.println(output+ "\n");
					}
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
////			System.out.println(System.nanoTime());
//			for(Entry<String, Supervisor> s : workers.entrySet())
//				System.out.println(s.getValue().toString());
			
			conn.disconnect();
		
	}
	
}
