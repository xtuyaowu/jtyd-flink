package quickstart.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import quickstart.pojo.KafkaConsumerTest;

import java.util.Properties;

public class StreamTimeWindowExample {
    public static void main(String[] args) throws Exception {

        // 作业名称
        String jobname = "StreamTimeWindowExample";
        //获取运行环境的上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka消费者配置
        Properties consumerProp = new Properties();
        String consumerBootstrapServers = "master.northking.com:9092,slave1.northking.com:9092,slave2.northking.com:9092";
        String groupId = "ai-group";
        String consumerTopic = "test";
        consumerProp.setProperty("bootstrap.servers", consumerBootstrapServers);
        consumerProp.setProperty("group.id", groupId);

        // 非常关键，一定要设置启动检查点！！
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 流处理时间特性设置为：IngestionTime 当时间设置为EventTime时要使用窗口函数必须指定wartermark
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // 创建kafka消费者，读入kafka消息
        FlinkKafkaConsumer<String> kafkaConsumer = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<String>(
                consumerTopic,
                new SimpleStringSchema(),
                consumerProp
        );
                //.assignTimestampsAndWatermarks(new MessageWaterEmitterPunctuate());   //递增时间戳分配器
        //周期性水印生成的另一个例子是当水印滞后的最大时间戳在数据流中被认为是一个固定的时间，在这种情况下
        // ，在数据流中遇到的最大延迟是已知的，例如，创建一个带时间戳的并在一个固定的时间内传播的元素的测试源
        // 。对于这些情况，Flink 提供了BoundedOutOfOrdernessTimestampExtractor，以maxOutOfOrderness作为参数
        // ，这个maxOutOfOrderness是指在窗口计算的最后，一个元素允许的最大延迟时间。延迟与t-t_w的结果相对应，
        // 这里t指的是元素的timestamp，而t_w指的是上个水印。如果延迟>0 那么这个元素被认为是延迟的，默认情况下，
        // 这个元素不计入窗口的最终计算中
        // .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessMsgWaterEmitterPeriodic(Time.seconds(10)));





        env.addSource(kafkaConsumer).map(new MapFunction<String, Tuple2<String,Double>>() {
            @Override
            public Tuple2<String,Double> map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                KafkaConsumerTest consumerTest = JSON.toJavaObject(jsonObject, KafkaConsumerTest.class);
                Tuple2<String, Double> tuple2 = new Tuple2<>();
                tuple2.f0 = consumerTest.getId();
                tuple2.f1 = consumerTest.getAmount();
                return tuple2;
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(10),Time.seconds(1))
                .sum(1)
                .print();


        env.execute(jobname);

    }
}
