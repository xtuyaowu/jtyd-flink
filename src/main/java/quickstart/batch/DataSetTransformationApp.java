package quickstart.batch;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import util.DBUtils;

public class DataSetTransformationApp {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//mapFunction(env);
		//filterFunction(env);
		//mapPartitionFunction(env);
		//firstFunction(env);
		//flatMapFunction(env);
		//distinctFunction(env);
		//joinFunction(env);
		//outerJoinFunction(env);
		crossFunction(env);
	}
	
	
	private static void crossFunction(ExecutionEnvironment env) throws Exception {
		List<String> info1 = new ArrayList<String>();
		info1.add("曼联");
		info1.add("曼城");
		List<String> info2 = new ArrayList<String>();
		info2.add("3");
		info2.add("1");
		info2.add("0");
		
		DataSource<String> data1 = env.fromCollection(info1);
		DataSource<String> data2 = env.fromCollection(info2);
		
		data1.cross(data2).print();
	}
	
	
	private static void outerJoinFunction(ExecutionEnvironment env) throws Exception {
		List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
		info1.add(new Tuple2(1, "PK哥"));
		info1.add(new Tuple2(2, "J哥"));
		info1.add(new Tuple2(3, "小队长"));
		info1.add(new Tuple2(4, "猪头哥"));
		
		List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
		info2.add(new Tuple2(1, "北京"));
		info2.add(new Tuple2(2, "上海"));
		info2.add(new Tuple2(3, "成都"));
		info2.add(new Tuple2(5, "杭州"));
		
		DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
		DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);
		
//		data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>,Tuple2<Integer, String>,Tuple3<Integer, String, String>>() {
//			@Override
//			public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second)
//					throws Exception {
//				// TODO Auto-generated method stub
//				if (second == null) {
//					return new Tuple3<Integer, String, String>(first.f0,first.f1,"-");
//				} else {
//					return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
//				}
//				
//			}
//		}).print();
		
//		data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>,Tuple2<Integer, String>,Tuple3<Integer, String, String>>() {
//			@Override
//			public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second)
//					throws Exception {
//				// TODO Auto-generated method stub
//				if (first == null) {
//					return new Tuple3<Integer, String, String>(second.f0,"-",second.f1);
//				} else {
//					return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
//				}
//				
//			}
//		}).print();
		
		data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>,Tuple2<Integer, String>,Tuple3<Integer, String, String>>() {
			@Override
			public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second)
					throws Exception {
				// TODO Auto-generated method stub
				if (first == null) {
					return new Tuple3<Integer, String, String>(second.f0,"-",second.f1);
				} else if (second == null) {
					return new Tuple3<Integer, String, String>(first.f0,first.f1,"-");
				} else {
					return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
				}
				
			}
		}).print();
	}
	
	
	private static void joinFunction(ExecutionEnvironment env) throws Exception {
		List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
		info1.add(new Tuple2(1, "PK哥"));
		info1.add(new Tuple2(2, "J哥"));
		info1.add(new Tuple2(3, "小队长"));
		info1.add(new Tuple2(4, "猪头哥"));
		
		List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
		info2.add(new Tuple2(1, "北京"));
		info2.add(new Tuple2(2, "上海"));
		info2.add(new Tuple2(3, "成都"));
		info2.add(new Tuple2(5, "杭州"));
		
		DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
		DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);
		
		data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>,Tuple2<Integer, String>,Tuple3<Integer, String, String>>() {

			@Override
			public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
			}
		}).print();
		  
	}
	
	private static void distinctFunction(ExecutionEnvironment env) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		list.add("hadoop,spark");
		list.add("hadoop,flink");
		list.add("flink,flink");
		DataSource<String> data = env.fromCollection(list);
		
		data.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public void flatMap(String input, Collector<String> collector) throws Exception {
				// TODO Auto-generated method stub
				String[] lists = input.split(",");
				for (String list : lists) {
					collector.collect(list);
				}
			}
		}).distinct().groupBy(0).sum(1).print();
		
	}


	private static void flatMapFunction(ExecutionEnvironment env) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		list.add("hadoop,spark");
		list.add("hadoop,flink");
		list.add("flink,flink");
		DataSource<String> data = env.fromCollection(list);
		
		data.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public void flatMap(String input, Collector<String> collector) throws Exception {
				// TODO Auto-generated method stub
				String[] lists = input.split(",");
				for (String list : lists) {
					collector.collect(list);
				}
			}
		}).map(new MapFunction<String, Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Integer> map(String input) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(input,1);
			}
		}).groupBy(0).sum(1).print();
		
	}

	private static void firstFunction(ExecutionEnvironment env) throws Exception {
		// TODO Auto-generated method stub
		List<Tuple2<Integer, String>> info = new ArrayList<Tuple2<Integer, String>>();
		info.add(new Tuple2(1, "hadoop"));
		info.add(new Tuple2(1, "spark"));
		info.add(new Tuple2(1, "flink"));
		info.add(new Tuple2(2, "java"));
		info.add(new Tuple2(2, "spring boot"));
		info.add(new Tuple2(3, "linux"));
		info.add(new Tuple2(4, "vue"));
		DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);
		data.first(3).print();
		data.groupBy(0).first(2).print();
		data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
		
	}

	private static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
		List<String> list = new ArrayList<String>();
		for (int i = 0; i < 100; i++) {
			list.add("student: "+i);
		}	
		//setParallelism 设置并行度
		DataSource<String> data = env.fromCollection(list).setParallelism(6);
		
//		data.map(new MapFunction<String, String>() {
//
//			@Override
//			public String map(String input) throws Exception {
//				// TODO Auto-generated method stub
//				String connection = DBUtils.getConnection();
//				System.out.println("connection=["+connection+"]");
//				DBUtils.returnConnection(connection);
//				return input;
//			}
//		}).print();
		
		data.mapPartition(new MapPartitionFunction<String, String>() {

			@Override
			public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
				// TODO Auto-generated method stub
				String connection = DBUtils.getConnection();
				System.out.println("connection=["+connection+"]");
				DBUtils.returnConnection(connection);		
			}
		}).print();
	}
	
	
	private static void filterFunction(ExecutionEnvironment env) throws Exception {
		List<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < 10; i++) {
			list.add(i);
		}
		
		DataSource<Integer> data = env.fromCollection(list);
		data.map(new MapFunction<Integer, Integer>()  {

			public Integer map(Integer input) throws Exception{
				return input+1;
			}
		})
		.filter(new FilterFunction<Integer>() {
			
			@Override
			public boolean filter(Integer input) throws Exception {
				// TODO Auto-generated method stub
				return input>5;
			}
		} )
		.print();

	}
	
	
	private static void mapFunction(ExecutionEnvironment env) throws Exception {
		List<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < 10; i++) {
			list.add(i);
		}
		
		DataSource<Integer> data = env.fromCollection(list);
		data.map(new MapFunction<Integer, Integer>()  {

			public Integer map(Integer input) throws Exception{
				return input+1;
			}
		}).print();
	}
}
