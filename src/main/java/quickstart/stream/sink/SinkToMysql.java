package quickstart.stream.sink;




import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;



import quickstart.pojo.Student;

public class SinkToMysql extends RichSinkFunction<Student>{
	Connection connection;
	PreparedStatement pStatement;

	private Connection getConnection(){
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String Url = "jdbc:mysql://172.16.113.231:3306/test";
			conn = DriverManager.getConnection(Url, "hnabidev", "hnabidev");
			conn.setAutoCommit(false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// TODO Auto-generated method stub
		return conn;
	}
	
	/*
	 * open方法中建立连接
	 * */
	@Override
	public void open(Configuration parameters) throws Exception {
		// TODO Auto-generated method stub
		super.open(parameters);
		connection = getConnection();
		String sqlString = "insert into student(id,name,age) values (?,?,?)";
		pStatement = connection.prepareStatement(sqlString);
		System.out.println("open");
	}
	
	/*
	 * 每条记录插入时调用一次*/
	@Override
	public void invoke(Student value, Context context) throws Exception {
		// TODO Auto-generated method stub
		super.invoke(value, context);
		System.out.println("invoke~~~~~~~~~~~~~~~");
		//为前面的占位符赋值
		pStatement.setInt(1, value.getId());
		pStatement.setString(2, value.getName());
		pStatement.setInt(3, value.getAge());

		int result = pStatement.executeUpdate();
		System.out.println(result);
		connection.commit();
		
		
	}


	
	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		super.close();
		connection.close();

	}

	

}
