package quickstart.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import quickstart.pojo.KafkaConsumerTest;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Description    MessageWaterEmitterPunctuate 根据Kafka消息确定Flink的水位
 * @Author         jichangyu
 * @CreateDate     2019/11/28 18:50
 * 数据流中每一个递增的EventTime都会产生一个Watermark。
 * 在实际的生产中，在TPS很高的场景下会产生大量的Watermark在一定程度上对下游算子造成压力，
 * 所以只有在实时性要求非常高的场景才会选择间断的方式进行水印的生成,
 * 更精准，但是大数据量的情况下会影响性能
 */
public class PunctuatedMessageWaterEmitterPunctuate implements AssignerWithPunctuatedWatermarks<String> {
    private Long currentMaxTimestamp = 0l;
    //水印延迟时间
    private static final Long DELAY_TIME = 3 * 1000l;

    /**
     * 再执行该函数，extractedTimestamp的值是方法extractTimestamp()的返回值
     *
     * @param lastElement        数据流元素
     * @param extractedTimestamp
     * @return
     */

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        if(lastElement != null && lastElement.contains(",")) {
//            String[] split = lastElement.split(",");
//            return new Watermark(Long.parseLong(split[0]));

//            JSONObject jsonObject = JSON.parseObject(lastElement);
//            KafkaConsumerTest consumerTest = JSON.toJavaObject(jsonObject, KafkaConsumerTest.class);
//            long time = 0;
//            SimpleDateFormat sdfLong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            try {
//                 time = sdfLong.parse(consumerTest.getMassage_timestamp()).getTime();
//            } catch (ParseException e) {
//                e.printStackTrace();
//            }
//            System.out.println("a："+time);
//            return new Watermark(time);

            return new Watermark(currentMaxTimestamp-DELAY_TIME);
        }

        return null;
    }

    /**
     * 先执行该函数，从element中提取时间戳
     *
     * @param element          数据流元素
     * @param previousElementTimestamp 当前的系统时间
     * @return 数据的事件时间戳，触发器Trigger中的时间也是这个返回值
     */
    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        if(element != null && element.contains(",")){
            //String[] split = element.split(",");
            //return Long.parseLong(split[0]);

            JSONObject jsonObject = JSON.parseObject(element);
            KafkaConsumerTest consumerTest = JSON.toJavaObject(jsonObject, KafkaConsumerTest.class);
            long time = 0;
            SimpleDateFormat sdfLong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                time = sdfLong.parse(consumerTest.getMassage_timestamp()).getTime();
                currentMaxTimestamp = Math.max(time, previousElementTimestamp);

            } catch (ParseException e) {
                e.printStackTrace();
            }
            System.out.println("b："+time);
            return time;
        }
        return 0;
    }
}
