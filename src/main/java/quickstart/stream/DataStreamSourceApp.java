package quickstart.stream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import quickstart.stream.source.CustomNonParallelSourceFunction;
import quickstart.stream.source.CustomParallelSourceFunction;
import quickstart.stream.source.CustomRichParallelSourceFunction;

public class DataStreamSourceApp {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		//socketFunction(env);
		//nonParallelSourceFunction(env);  //可以用于模拟数据
		//parallelSourceFunction(env);//可以用于模拟数据
		richParallelSourceFunction(env);//可以用于模拟数据
		env.execute("DataStreamSourceApp");

	}

	
	private static void richParallelSourceFunction(StreamExecutionEnvironment env) {
		// TODO Auto-generated method stub
		DataStreamSource<Long> data = env.addSource(new CustomRichParallelSourceFunction()).setParallelism(2);
		data.print().setParallelism(1);
	}
	
	private static void parallelSourceFunction(StreamExecutionEnvironment env) {
		// TODO Auto-generated method stub
		DataStreamSource<Long> data = env.addSource(new CustomParallelSourceFunction()).setParallelism(2);
		data.print().setParallelism(1);
	}

	private static void nonParallelSourceFunction(StreamExecutionEnvironment env) {
		// TODO Auto-generated method stub
		DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunction());
		data.print().setParallelism(1);
	}

	private static void socketFunction(StreamExecutionEnvironment env) {
		// TODO Auto-generated method stub
		DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
		data.print().setParallelism(1);
	}
}
