package quickstart.stream;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import quickstart.stream.source.CustomNonParallelSourceFunction;

public class DataStreamTransformationApp {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// filterFunction(env);
		// unionFunction(env);
		splitSelectFunction(env);
		env.execute("DataStreamTransformationApp");
	}

	private static void splitSelectFunction(StreamExecutionEnvironment env) {
		// TODO Auto-generated method stub
		DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunction());
		SplitStream<Long> splits = data.split(new OutputSelector<Long>() {
			
			@Override
			public Iterable<String> select(Long value) {
				// TODO Auto-generated method stub
				ArrayList<String> output = new ArrayList<String>();
				if(value % 2 == 0) {
					output.add("even");
				}else {
					output.add("odd");
				}
				return output;
			}
		});
		
		splits.select("odd").print().setParallelism(1);
		
	}

	private static void unionFunction(StreamExecutionEnvironment env) {
		// TODO Auto-generated method stub
		DataStreamSource<Long> data1 = env.addSource(new CustomNonParallelSourceFunction());
		DataStreamSource<Long> data2 = env.addSource(new CustomNonParallelSourceFunction());
		data1.union(data2).print().setParallelism(1);

	}

	private static void filterFunction(StreamExecutionEnvironment env) {
		// TODO Auto-generated method stub
		DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunction());
		data.map(new MapFunction<Long, Long>() {

			@Override
			public Long map(Long value) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("value = [" + value + "]");
				return value;
			}
		}).filter(new FilterFunction<Long>() {

			@Override
			public boolean filter(Long value) throws Exception {
				// TODO Auto-generated method stub
				return value % 2 == 0;
			}
		}).print().setParallelism(1);

	}

}
