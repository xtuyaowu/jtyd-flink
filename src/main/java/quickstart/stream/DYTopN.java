package quickstart.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import quickstart.function.AscendingMsgWaterEmitterPeriodic;


import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ：jichangyu
 * @date ：Created in 2020/5/18 10:07
 * @description：抖音当天TOPN统计
 * @modified By：
 * @version: 1.0
 */
public class DYTopN {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Long checkpointInterval = parameterTool.getLong("checkpointInterval", 1000);

        String cBootstrapServers = parameterTool.get("cBootstrapServers", "192.168.1.102:9092");
        String groupId = parameterTool.get("groupId", "jcyFlinkGroup");
        String consumerTopic = parameterTool.get("consumerTopic", "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        Properties cProperties = new Properties();
        cProperties.setProperty("bootstrap.servers", cBootstrapServers);
        cProperties.setProperty("group.id", groupId);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(consumerTopic
                , new SimpleStringSchema()
                , cProperties);
        consumer.setStartFromEarliest();

        SingleOutputStreamOperator<String> out = env.addSource(consumer)
              //增加水印，允许数据延迟15秒
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(15)) {
            @Override
            public long extractTimestamp(String element) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date eventTime = null;
                try {
                    String[] split = element.split(",");
                    eventTime = format.parse(split[0].toString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return eventTime.getTime();
            }
        });

//方法零：需要缓存一天的数据
//                SingleOutputStreamOperator<Tuple2<String, Integer>> process = out.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String value) throws Exception {
//                String[] splits = value.split(",");
//                Tuple2<String, Integer> myTuple = new Tuple2<>();
//                SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
//                myTuple.f0 = splits[1].toString();
//                myTuple.f1 = 1;
//                return myTuple;
//            }
//        }).windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
////               .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
//                .trigger(CountTrigger.of(1))
//                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String,Integer>, TimeWindow>() {
//                    @Override
//                    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2< String, Integer>> out) throws Exception {
//                        Set<String> uvNameSet=new HashSet<String>();
//                        Integer count=0;
//                        Iterator<Tuple2<String,Integer>> mapIterator=elements.iterator();
//                        while(mapIterator.hasNext()){
//                            count+=1;
//                            mapIterator.next();
//                        }
//                        out.collect(Tuple2.of("访问量",count));
//                    }
//                });


        SingleOutputStreamOperator<Tuple2<String, Integer>> process = out.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] splits = value.split(",");
                Tuple2<String, Integer> myTuple = new Tuple2<>();
                SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
                //如果需要日期，请拼接上日期
                myTuple.f0 = format.format(format.parse(splits[0].toString())) + "-" + splits[1].toString();
                //myTuple.f0 = splits[1].toString();
                myTuple.f1 = 1;
                return myTuple;
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        })
                //每天零点滚动一个窗口
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                //.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                /*这是使用state是因为，窗口默认只会在创建结束的时候触发一次计算，然后数据结果，
                如果长时间的窗口，比如：一天的窗口，要是等到一天结束在输出结果，那还不如跑批。
                所有大窗口会添加trigger，以一定的频率输出中间结果。(每10秒触发一次或者每条触发一次)
                加evictor 是因为，每次trigger，触发计算是，窗口中的所有数据都会参与，所以数据会触发很多次，比较浪费，加evictor 驱逐已经计算过的数据，就不会重复计算了
                驱逐了已经计算过的数据，导致窗口数据不完全，所以需要state 存储我们需要的中间结果
                */

                //方法一：直接计算-原始数据特别大，TumblingEventTimeWindows会缓存一天数据，每次重新计算，内存吃不消
                // .sum(1);
                //方法二：key如果很多，MapState太大了，而且要每次遍历
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    private transient MapState<String, Integer> countState;
                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        StateTtlConfig ttlConfig = StateTtlConfig
                                                    .newBuilder(org.apache.flink.api.common.time.Time.minutes(60 * 6))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();

                        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("sum", String.class, Integer.class);
                        descriptor.enableTimeToLive(ttlConfig);
                        countState = getRuntimeContext().getMapState(
                                descriptor);
                   //     state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Integer.class));

                    }

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer count = 0;
                        Integer originCount = countState.get(s); //
                        //Integer originCount = state.value();
                        if (originCount == null) {
                            originCount = 0;
                            countState.put(s,originCount); //
                        }


                        //System.out.println("原始值"+originCount);
                        for (Tuple2<String, Integer> in: elements) {
                        count++;
                        }
                        //System.out.println("新值:"+count);
                        countState.put(s,originCount+count); //
                        //state.update(originCount+count);
                        Tuple2<String, Integer> tuple2 = new Tuple2<>();
                        tuple2.f0 =s;
                        tuple2.f1 = originCount+count;
                        out.collect( tuple2);
                    }

                    @Override
                    public void clear(Context context) throws Exception {
                        super.clear(context);
                    }
                });
                //方法三：采用BoomFilter
//                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
//
//                    //import com.google.common.hash.BloomFilter;
//                    //import com.google.common.hash.Funnels;
////                    private transient MapState<String, Integer> countState;
//                    private transient ValueState<BloomFilter<String>> boomFilterState;
//                    private ValueState<Integer> state;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        StateTtlConfig ttlConfig = StateTtlConfig
//                                .newBuilder(org.apache.flink.api.common.time.Time.minutes(60 * 6))
//                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                                .build();
//
////                        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("sum", String.class, Integer.class);
////                        descriptor.enableTimeToLive(ttlConfig);
//
//                        ValueStateDescriptor<BloomFilter<String>> boomFilterDescriptor = new ValueStateDescriptor<BloomFilter<String>>("boom_filter", TypeInformation.of(new TypeHint<BloomFilter<String>>() {}));
//                        boomFilterDescriptor.enableTimeToLive(ttlConfig);
//                        boomFilterState = getRuntimeContext().getState(boomFilterDescriptor);
////                        countState = getRuntimeContext().getMapState(
////                                descriptor);
//                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Integer.class));
//
//                    }
//
//                    @Override
//                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Integer count = 0;
//                        //Integer originCount = countState.get(s);
//                        Integer originCount = state.value();
////                        if (originCount==null){
////                            originCount =0;
////                            state.update(originCount);
////                        }
////                        System.out.println("haha");
//                        BloomFilter<String> bloomFilter = boomFilterState.value();
////                        bloomFilter.toString();
//                        if (bloomFilter == null) {
//                            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 10*1000*1000);
//                            originCount = 0;
//                        }
//
//
//                        //System.out.println("原始值"+originCount);
//                        for (Tuple2<String, Integer> in: elements) {
//                            count++;
//                            if (!bloomFilter.mightContain(s)) {
//                                bloomFilter.put(s); //不包含就添加进去
//                            }
//                        }
//                        //System.out.println("新值:"+count);
//                        //countState.put(s,originCount+count);
//                        boomFilterState.update(bloomFilter);
//                        state.update(originCount+count);
//                        Tuple2<String, Integer> tuple2 = new Tuple2<>();
//                        tuple2.f0 =s;
//                        tuple2.f1 = originCount+count;
//                        out.collect( tuple2);
//                    }
//
//                    @Override
//                    public void clear(Context context) throws Exception {
//                        super.clear(context);
//                    }
//                });


        process.print();
        // sum.print();



        env.execute("douyintest");
    }
}
