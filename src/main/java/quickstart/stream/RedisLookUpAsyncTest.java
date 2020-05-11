package quickstart.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import quickstart.function.RedisAsyncLookupTableSource;

import java.util.Properties;
 
public class RedisLookUpAsyncTest {

 
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
 
        final ParameterTool params = ParameterTool.fromArgs(args);
        String fileName = params.get("f");
        DataStream<String> source = env.readTextFile("hdfs://172.16.44.28:8020" + fileName, "UTF-8");
 
        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG};
        String[] fields = new String[]{"id", "user_click", "time"};
        RowTypeInfo typeInformation = new RowTypeInfo(types, fields);
 
        DataStream<Row> stream = source.map(new MapFunction<String, Row>() {
            private static final long serialVersionUID = 2349572543469673349L;
 
            @Override
            public Row map(String s) {
                String[] split = s.split(",");
                Row row = new Row(split.length);
                for (int i = 0; i < split.length; i++) {
							
                    Object value = split[i];
                    if (types[i].equals(Types.STRING)) {
                        value = split[i];
                    }
                    if (types[i].equals(Types.LONG)) {
                        value = Long.valueOf(split[i]);
                    }
                    row.setField(i, value);
                }
                return row;
            }
        }).returns(typeInformation);
 
        tableEnv.registerDataStream("user_click_name", stream, String.join(",", typeInformation.getFieldNames()) + ",proctime.proctime");
 
        RedisAsyncLookupTableSource tableSource = RedisAsyncLookupTableSource.Builder.newBuilder()
                .withFieldNames(new String[]{"id", "name"})
                .withFieldTypes(new TypeInformation[]{Types.STRING, Types.STRING})
                .build();
        tableEnv.registerTableSource("info", tableSource);
 
        String sql = "select t1.id,t1.user_click,t2.name" +
                " from user_click_name as t1" +
                " join info FOR SYSTEM_TIME AS OF t1.proctime as t2" +
                " on t1.id = t2.id";
 
        Table table = tableEnv.sqlQuery(sql);
 
        DataStream<Row> result = tableEnv.toAppendStream(table, Row.class);
 
        DataStream<String> printStream = result.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row value) throws Exception {
                return value.toString();
            }
        });
 
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9094");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "user_click_name",  
                new SimpleStringSchema(),
                properties);
        printStream.addSink(kafkaProducer);
 
        tableEnv.execute(Thread.currentThread().getStackTrace()[1].getClassName());
    }
}