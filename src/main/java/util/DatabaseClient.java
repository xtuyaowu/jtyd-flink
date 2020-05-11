package util;

import org.apache.flink.shaded.netty4.io.netty.util.concurrent.Future;

public class DatabaseClient {
	String host;
	String post;
	String credentials;
	
	public DatabaseClient(String host, String post, String credentials) {
		super();
		this.host = host;
		this.post = post;
		this.credentials = credentials;
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public Future<String> query(String key) {
		// TODO Auto-generated method stub
		return null;
	}

}
