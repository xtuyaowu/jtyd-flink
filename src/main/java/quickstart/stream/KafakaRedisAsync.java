package quickstart.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import quickstart.stream.function.AsyncFunctionRedisRequest;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class KafakaRedisAsync {
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

        SingleOutputStreamOperator<String> source = env.addSource(consumer).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
//                JSONObject jsonObject = JSON.parseObject(value);
//                Set<String> strings = jsonObject.keySet();
                if (!value.contains("id")) {
                    return false;
                }
                return true;
            }
        });
        //source.print();
        SingleOutputStreamOperator<String> result = AsyncDataStream.unorderedWait(source, new AsyncFunctionRedisRequest(), 5, TimeUnit.SECONDS, 10);
        //5.输出到 目标
        result.print();


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
