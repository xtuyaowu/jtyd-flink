package quickstart.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import quickstart.pojo.DDLDemo;

/**
 * @author ：jichangyu
 * @date ：Created in 2020/4/21 10:56
 * @description：FlinkKafkaDDLDemo
 * @modified By：
 * @version: 1.0
 */
public class FlinkKafkaDDLDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, build);

        //创建kafka源表
        String createTable ="CREATE TABLE PERSON (\n" +
                "name VARCHAR COMMENT '姓名',\n" +
                "age VARCHAR COMMENT '年龄',\n" +
                "city VARCHAR COMMENT '所在城市',\n" +
                "address VARCHAR COMMENT '家庭住址',\n" +
                "ts BIGINT COMMENT '时间戳',\n" +
                "t as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "proctime as PROCTIME(),\n" +
                "WATERMARK FOR t AS t - INTERVAL '5' SECOND\n" +
                ")\n" +
                "WITH (\n" +
                "'connector.type' = 'kafka', -- 使用 kafka connector\n" +
                "'connector.version' = 'universal',  -- kafka 版本\n" +
                "'connector.topic' = 'test',  -- kafka topic\n" +
                "'connector.startup-mode' = 'latest-offset', -- 从起始 offset 开始读取\n" +
                "'connector.properties.zookeeper.connect' = '192.168.1.102:2181',  -- zk连接信息\n" +
                "'connector.properties.bootstrap.servers' = '192.168.1.102:9092',  -- broker连接信息\n" +
                "'connector.properties.group.id' = 'jcyFlinkGroup',\n" +
                "'update-mode' = 'append',\n" +
                "'format.type' = 'json',  -- 数据源格式为 json\n" +
                "'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则\n" +
                ")";

        tEnv.sqlUpdate(createTable);

        String query = "SELECT name,\n" +
                "cast(COUNT(age) as int) as pv,\n" +
                "cast(TUMBLE_START(t, INTERVAL '5' second) as string) as st_tart,\n" +
                "cast(TUMBLE_END(t, INTERVAL '5' second) as string) as t_end,\n" +
                "cast(count(distinct age) as int) as uv\n" +
                "FROM PERSON\n" +
                "GROUP BY name,\n" +
                "TUMBLE(t, INTERVAL '5' second)";

       // String query = "SELECT name FROM PERSON";
        Table result = tEnv.sqlQuery(query);
        tEnv.toRetractStream(result, DDLDemo.class).print();


        env.execute("FLINK SQL DDL");


    }
}
