package util;

import java.util.Random;

public class DBUtils {

	public static String getConnection() {
		Random ran = new Random();
		// TODO Auto-generated method stub
		return "connect:"+ran.nextInt(100);
	}

	public static void returnConnection(String connection) {
		// TODO Auto-generated method stub
		
	}


}
