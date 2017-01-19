package general;

import java.time.Instant;

public class Methods {
	public static String formattime(){
		Instant instant = Instant.now (); // Current date-time in UTC.
		String output = instant.toString ();
		output = instant.toString ().replace ( "T" , " " ).replace( "Z" , "");
		return output;
	}
}
