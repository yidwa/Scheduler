package general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.storm.scheduler.SupervisorDetails;

public class test {
	public static void main(String[] args) {
//		Random rd = new Random(); 
		test tt = new test();
		ArrayList<Integer> t = new ArrayList<>();
		t.add(8);
		t.add(0);
		t.add(8);
		tt.testing(t);
		
	}
	public void testing(ArrayList<Integer> t){
		ArrayList<Integer> updatelist = new ArrayList<>();
		int[] avail = new int[t.size()];
		int minind = 0;
		int maxind = avail.length-1;
		
			for(int i = 0 ; i<t.size(); i++){
				avail[i] = t.get(i);
				if(avail[i]<avail[minind])
					minind = i;
				else if(avail[i]>=avail[maxind])
					maxind = i;
			}
			System.out.println(maxind + " , "+minind);
			if(avail.length==3){
				updatelist.add(t.get(maxind));
				for(int j = 0; j<3; j++){
						if(j!=maxind && j!= minind){
							System.out.println("find j "+j+" , "+t.get(j));
							updatelist.add(t.get(j));
						}
					}
				updatelist.add(t.get(minind));
			}
			else{
				updatelist.add(t.get(maxind));
				updatelist.add(t.get(minind));
			}
			System.out.println(updatelist.toString());
	}
}