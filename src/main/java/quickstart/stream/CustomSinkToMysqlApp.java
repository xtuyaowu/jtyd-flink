package quickstart.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import quickstart.pojo.Student;
import quickstart.stream.sink.SinkToMysql;

public class CustomSinkToMysqlApp {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> source = env.socketTextStream("192.168.171.129", 9999);
		//source.print().setParallelism(1);
		SingleOutputStreamOperator<Student> studentStream = source.map(new MapFunction<String, Student>() {

			@Override
			public Student map(String value) throws Exception {
				// TODO Auto-generated method stub
				String[] splits = value.split(",");
				Student stu = new Student();
				stu.setId(Integer.parseInt(splits[0]));
				stu.setName(splits[1]);
				stu.setAge(Integer.parseInt(splits[2]));
				return stu;
			}
			
		});
		
		studentStream.addSink(new SinkToMysql()).setParallelism(1);
		
		env.execute("CustomSinkToMysqlApp");
		
	}
}
