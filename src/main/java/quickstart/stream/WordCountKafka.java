package quickstart.stream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import quickstart.pojo.WordCount;
import util.WordCountData;

public class WordCountKafka {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		

		//开启checkPoint, Time interval between state checkpoints 5000 milliseconds.
	    /**
	      * 如果我们启用了Flink的Checkpint机制，
	      * 那么Flink Kafka Consumer将会从指定的Topic中消费消息，
	      * 然后定期地将Kafka offsets信息、状态信息以及其他的操作信息进行Checkpint。
	      * 所以，如果Flink作业出故障了，Flink将会从最新的Checkpint中恢复，
	      * 并且从上一次偏移量开始读取Kafka中消费消息。
	      */
	    env.enableCheckpointing(1000);
	    
	    //设置系统基本时间特性为事件时间
	   // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		FlinkKafkaConsumer<String> consumer=null;
		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			Properties props = new Properties();
	        props.setProperty("bootstrap.servers", "c01:9092");
	       // props.setProperty("zookeeper.connect", "192.168.171.129:2181,192.168.171.130:2181，192.168.171.131:2181");
	        props.setProperty("group.id", "flink-group");
	        /** 注释2
	         * 下面配置读取kafka topic partition 的起始偏移量（可选）
	         * consumer.setStartFromEarliest();
	         * consumer.setStartFromLatest();
	         * consumer.setStartFromTimestamp(...);
	         * consumer.setStartFromGroupOffsets();  // 默认
	        */

		    consumer =new FlinkKafkaConsumer<>("mytopic", new SimpleStringSchema(), props);
//		    1. TypeInformationSerializationSchema (andTypeInformationKeyValueSerializationSchema) ，他们会基于Flink的TypeInformation来创建schema。这对于那些从Flink写入，又从Flink读出的数据是很有用的。这种Flink-specific的反序列化会比其他通用的序列化方式带来更高的性能。
//
//		    2. JsonDeserializationSchema (andJSONKeyValueDeserializationSchema) 可以把序列化后的Json反序列化成ObjectNode，ObjectNode可以通过objectNode.get(“field”).as(Int/String/…)() 来访问指定的字段。
//
//		    3. SimpleStringSchema可以将消息反序列化为字符串。当我们接收到消息并且反序列化失败的时候，会出现以下两种情况: 1) Flink从deserialize(..)方法中抛出异常，这会导致job的失败，然后job会重启；2) 在deserialize(..) 方法出现失败的时候返回null，这会让Flink Kafka consumer默默的忽略这条消息。请注意，如果配置了checkpoint 为enable，由于consumer的失败容忍机制，失败的消息会被继续消费，因此还会继续失败，这就会导致job被不断自动重启。
//		
		}
		

//		1. 构造函数 ，需要三个参数：
//
//		topic 名
//		反序列化方法
//		Kafka consumer的Properties
//		2. checkpoint，从注释1 可以看出，Flink Kafka Consumer 可以启动checkpoint机制，会周期性的给Kafka offsets 和 Flink的其它states 做 checkpoints，当Job 失败时，Flink 读取checkpoint里最新的state并从对应offset 开始消费数据恢复运算。
//
//		3. 起始偏移量，从注释2可以看出，Flink Kafka Consumer 允许设置读取 Kafka Partition的起始偏移量，而且，允许不同Partitions 分别进行设置。但要注意，设置起始偏移量不适用于两种情况：
//
//		Job 从failure 自动恢复。
//		手动从某savepoint 启动任务。
//		4. Kafka Topic 和Partition 自发现，比如构建Kafka Consumer时，topic名可以是正则表达式，这时候，如果有符合该正则的新的topic 加入到Kafka 集群，可以被自动发现；另外，如果对Kafka Topic 进行RePartition，也可以自动发现，使用不多，可以自行查阅文档。
//
//		5. Kafka Consumer与Watermark 
		/** 注释3
		 * Watermark （可选）
		 * consumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
		 */
		DataStream<Tuple2<String, Integer>> counts = env.addSource(consumer)
			// split up the lines in pairs (2-tuples) containing: (word,1)
			.flatMap(new Tokenizer())
			// group by the tuple field "0" and sum up tuple field "1"
			.keyBy(0).sum(1);

		// emit result
		if (params.has("output")) {
			counts.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			//counts.print();
			counts.addSink(new WorldCountSinkToMysql());
		}

		// execute program
		env.execute("Streaming WordCount");
	}


	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
	public static final class WorldCountSinkToMysql extends RichSinkFunction<Tuple2<String, Integer>> {
		Connection connection;
		PreparedStatement pStatement;

		private Connection getConnection(){
			Connection conn = null;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				String Url = "jdbc:mysql://localhost:3306/mydb";
				conn = DriverManager.getConnection(Url, "root", "891210");
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
			String sqlString = "insert into wordcount(word,counts,appendtime) values (?,?,?)";
			pStatement = connection.prepareStatement(sqlString);
			//System.out.println("open");
		}

		/*
		 * 每条记录插入时调用一次*/
		@Override
		public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
			// TODO Auto-generated method stub
			super.invoke(value, context);
			//System.out.println("invoke~~~~~~~~~~~~~~~");
			//为前面的占位符赋值
			pStatement.setString(1, value.f0);
			pStatement.setInt(2, value.f1);
			Date date = new Date();
			SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			pStatement.setString(3,dateFormat.format(date));
			//pStatement.setInt(3, value.getAge());

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

}