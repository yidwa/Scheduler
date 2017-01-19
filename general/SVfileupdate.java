package general;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map.Entry;

public class SVfileupdate implements Runnable {
	File sysinfo;
	FileWriter fw;
	HashMap<String,Supervisor> workers;
	
	public SVfileupdate(HashMap<String, Supervisor> workers) {
		// TODO Auto-generated constructor stub
		this.sysinfo = new File(Constants.sysinfoserverpath);
		this.workers = workers;
		try {
			this.fw = new FileWriter(sysinfo,true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
		//	System.out.println("write to file now");
			if (workers.size() == 0){
				fw.write("no supervisor is currently working");
				fw.flush();
			}
			else{
				for(Entry<String,Supervisor> e : workers.entrySet()){
					Supervisor s = e.getValue();
			//		System.out.println("host is "+ e.getKey());
					fw.write(e.getKey() + " ,  "+ Methods.formattime() + " , "+ s.cpuhis.toString() + " , "+ s.memhis.toString() + " , "+ s.usedslot+ "\n");
		//			System.out.println(" write "+ "CPU used list " + s.cpuhis.toString() + " , Memory used list "+ s.memhis.toString() + " slot used now "+ s.usedslot);
					fw.flush();
				}
			}
				fw.close();
		}
		catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	

}
