package quickstart.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.*;

public class KafakaToKafka {
    public static void main(String[] args) throws Exception {

        //1.获取参数
//        String fileName = args[0];
//        ParameterTool.fromPropertiesFile(fileName);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);


        Integer checkpointInterval = parameterTool.getInt("checkpointInterval", 1000);
        String cBootstrapServers = parameterTool.get("cBootstrapServers", "192.168.1.102:9092");
        String groupId = parameterTool.get("groupId", "jcyFlinkGroup");
        String consumerTopic = parameterTool.get("consumerTopic", "test");
        String pBootstrapServers = parameterTool.get("pBootstrapServers", "192.168.1.102:9092");
        String producerTopic = parameterTool.get("producerTopic", "PRODUCER_JYLSSS");
        //2.设置环境以及参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        // checkpint(检查点)设置
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        //streamEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
        //  streamEnv.setStateBackend(new FsStateBackend("file:///Users/temp/cp/"));


        //3.配置数据源、数据目的
        //3.1配置数据源
        Properties cProperties = new Properties();
        cProperties.setProperty("bootstrap.servers", cBootstrapServers);
        cProperties.setProperty("group.id", groupId);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(consumerTopic
                , new SimpleStringSchema()
                , cProperties);
        consumer.setStartFromEarliest();
        //  consumer.setCommitOffsetsOnCheckpoints(false);

        //3.2配置数据目的
        Properties pProperties = new Properties();
        pProperties.setProperty("bootstrap.servers", pBootstrapServers);
        pProperties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "10000");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(producerTopic
                , new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema())
                , pProperties
                , FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        //counts.addSink(producer);


        //4.数据处理

// 正常测试
//        DataStream<String> counts = env.addSource(consumer)
//                // split up the lines in pairs (2-tuples) containing: (word,1)
//                .flatMap(new KafakaToKafka.Tokenizer())
//                // group by the tuple field "0" and sum up tuple field "1"
//                .keyBy(0)
//                .sum(1).map(new MapFunction<Tuple2<String, Integer>, String>() {
//                    @Override
//                    public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                        return stringIntegerTuple2.toString();
//                    }
//                });

        //onTimer测试
        SingleOutputStreamOperator<Tuple2<String, Long>> process = env.addSource(consumer)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);//fastJOSN包配置为provide时，运行需要设置包含包，所以不建议设置为provide
                        Tuple2<String, Integer> tuple2 = new Tuple2<>();
                        tuple2.f0 = jsonObject.getString("NAME");
                        tuple2.f1 = Integer.valueOf(jsonObject.getString("HEARTBEAT"));
                        return tuple2;
                    }
                })
                .keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Long>>() {
                    /**
                     * 存储在state中的数据类型
                     */
                    class CountWithTimestamp {
                        public String key;
                        public long count;
                        public long lastModified;
                    }

                    //这个状态是通过ProcessFunction维护的
                    private ValueState<CountWithTimestamp> state;
                    HashMap<String, Long> hashMap = new HashMap<String, Long>();
                    Boolean first = true;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        //仅在该算子接收到第一个数据时，注册一个定时器
                        if (first) {
                            first = false;
                            long time = System.currentTimeMillis();
                            System.out.println("定时器第一次注册：" + time);
                            ctx.timerService().registerProcessingTimeTimer(time + 500);

                        }

                        //查看当前计数

//                        CountWithTimestamp current = state.value();
//                        if (current == null) {
//                            current = new CountWithTimestamp();
//                            current.key = value.f0;
//                        }
                        if (hashMap.get(value.f0) != null) {
                            hashMap.put(value.f0, hashMap.get(value.f0) + 1);
                        } else {
                            hashMap.put(value.f0, (long) 1);
                        }

                        //更新状态中的数
                        //current.count++;
                        //System.out.println(ctx.timestamp());
                        //设置状态的时间戳为记录的事件时间时间戳
                        //current.lastModified = System.currentTimeMillis();//ctx.timestamp();
                        System.out.println("记录进入处理:事件时间戳：" + System.currentTimeMillis()/*ctx.timestamp()*/);
                        //状态回写
                        //state.update(current);

                        //从当前事件时间开始注册一个为60s的定时器
                        //ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        System.out.println("定时器触发：" + timestamp);
                        System.out.println("定时器注册：" + timestamp);

                        //该定时器执行完任务之后，重新注册一个定时器
                        ctx.timerService().registerProcessingTimeTimer(timestamp + 5000);
                        //得到设置这个定时器的键对应的状态
                        //CountWithTimestamp result = state.value();
                        //out.collect(new Tuple2<String, Long>(result.key, result.count));
                        Long lisi = hashMap.get("LISI");

                        out.collect(new Tuple2<String,Long>("LISI",lisi));
//                        System.out.println("定时器的键对应的状态：" + result.lastModified);
//                        //检查定时器是过时定时器还是最新的定时器
//                        if (timestamp == result.lastModified + 60000) {
//                            System.out.println("触发定时器");
//                            //emit the state on timeout
//                            out.collect(new Tuple2<String, Long>(result.key, result.count));
//                        }
                    }
                });


        //5.输出到 目标
        //counts.print();
        process.print();


        // execute program

        //6.触发执行
        env.execute("Streaming WordCount");

    }


    /*
     *拆分计数
     */
//    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
//        @Override
//        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
//            // normalize and split the line
//            String[] tokens = value.toLowerCase().split("\\W+");
//
//            // emit the pairs
//            for (String token : tokens) {
//                if (token.length() > 0) {
//                    out.collect(new Tuple2<>(token, 1));
//                }
//            }
//        }
//    }
}
