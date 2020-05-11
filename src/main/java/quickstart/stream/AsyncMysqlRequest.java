package quickstart.stream;
import com.alibaba.fastjson.JSON;

import quickstart.pojo.User;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.concurrent.TimeUnit;


public class AsyncMysqlRequest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<ObjectNode> source = null;//new FlinkKafkaConsumer<>("async", new JsonNodeDeserializationSchema(), Common.getProp());

        // 接收kafka数据，转为User 对象
        DataStream<User> input = env.addSource(source).map(value -> {
            String id = value.get("id").asText();
            String username = value.get("username").asText();
            String password = value.get("password").asText();

            //return new User(id, username, password);
            return  null;
        });
        // 异步IO 获取mysql数据, timeout 时间 1s，容量 10（超过10个请求，会反压上游节点）
        DataStream async = null;
                //AsyncDataStream.unorderedWait(input, new AsyncFunctionForMysqlJava(), 1000, TimeUnit.MICROSECONDS, 10);

        async.map(user -> {

            return JSON.toJSON(user).toString();
        })
        .print();

        env.execute("asyncForMysql");

    }
}