package quickstart.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import quickstart.function.HbaseCellValuesScalaFunction;
import quickstart.pojo.KafkaConsumerTest;
import quickstart.pojo.KafkaProducerTest;

import java.util.Properties;

public class ReadKafkaAndHBaseToKafka {
    public static void main(String[] args) throws Exception {

        // 作业名称
        String jobname = "tableFunctionTest";

        // 创建flink流处理的上下文环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);


        // kafka消费者配置
        //{"id":"1","name":"Sun WuKong","amount":666,"massage_timestamp":"1574423920282"}
        Properties consumerProp = new Properties();
        String consumerBootstrapServers = "master.northking.com:9092,slave1.northking.com:9092,slave2.northking.com:9092";
        String groupId = jobname;
        String consumerTopic = "test";
        consumerProp.setProperty("bootstrap.servers", consumerBootstrapServers);
        consumerProp.setProperty("group.id", groupId);

        // kafka生产者配置
        Properties producerProp = new Properties();
        String producerBootstrapServers = "master.northking.com:9092,slave1.northking.com:9092,slave2.northking.com:9092";
        String producerTopic = "PRODUCER_SSMX";
        producerProp.setProperty("bootstrap.servers", producerBootstrapServers);
        producerProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "10000");

        // checkpint(检查点)设置
        streamEnv.enableCheckpointing(5000);
        streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 流处理时间特性设置为：IngestionTime
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // 创建kafka消费者，读入kafka消息
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                consumerTopic,
                new SimpleStringSchema(),
                consumerProp
        );

        // 创建kafka生产者，输出消息到kafka
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                producerTopic
                , new KeyedSerializationSchemaWrapper(new SimpleStringSchema())
                , producerProp
                , FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        // 开启生产者写入时间戳
        kafkaProducer.setWriteTimestampToKafka(true);


        // 读入kafka消息（JSON）
        SingleOutputStreamOperator<KafkaConsumerTest> input = streamEnv.addSource(kafkaConsumer).map(new MapFunction<String, KafkaConsumerTest>() {

            @Override
            public KafkaConsumerTest map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                KafkaConsumerTest consumerTest = JSON.toJavaObject(jsonObject, KafkaConsumerTest.class);
                return consumerTest;
            }
        });
        input.print();
        Table tableA = streamTableEnv.fromDataStream(input);
        streamTableEnv.registerTable("tableA", tableA);

        // 注册table函数
        // streamTableEnv.registerFunction("tableB", new HbaseCellValuesTableFunctionJYLSLSLJ());
        // 注册HBase取数函数
        streamTableEnv.registerFunction("GetCellValueFromHBase",new HbaseCellValuesScalaFunction());

        Table queryResult = streamTableEnv.sqlQuery("select A.id,A.name,'Fmale' as sex," +
                "cast(GetCellValueFromHBase('JYLSLSLJ','info','c0',A.id) as double) amount,'10' flag," +
                "cast(CURRENT_TIMESTAMP as varchar(30)) input_timestamp,massage_timestamp " +
                "FROM tableA as A WHERE A.amount > 300");
//可以通过再嵌套一层实现汇总计算(经测试不能使用)
//        streamTableEnv.registerTable("queryResult0", queryResult0);
//        Table queryResult = streamTableEnv.sqlQuery("select count(id) from queryResult0 group by id,name,sex,amount,input_timestamp,massage_timestamp");

        //"select id,name,amount,B.JYJE FROM tableA,LATERAL TABLE(tableB('KHJH','info',list,'1','1')) as B(KHTYBH,JYRQ,JYJE) ON tableA.id=B.KHTYBH WHERE amount < 300"
        //"select id,name,amount,'1' flag,cast(CURRENT_TIMESTAMP as varchar(30)) input_timestamp,massage_timestamp FROM tableA"
        DataStream<KafkaProducerTest> result = streamTableEnv.toAppendStream(queryResult, KafkaProducerTest.class);
        result.print();
        //轉成kafka消息（JSON）
//        SingleOutputStreamOperator<String> output = result.map(new MapFunction<KafkaProducerTest, String>() {
//            @Override
//            public String map(KafkaProducerTest value) throws Exception {
//                JSONObject jsonObject = new JSONObject();
//                String s = jsonObject.toJSONString(value);
//                return s;
//            }
//        });
//
//
//        // Sink到kafka
//        output//.addSink(kafkaProducer)
//        .print();
////                .name("Sink to kafka");

        // 提交并执行
        streamEnv.execute(jobname);
    }

}
