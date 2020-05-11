package quickstart.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import quickstart.pojo.KafkaConsumerTest;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Description    MessageWaterEmitterPunctuate 根据Kafka消息确定Flink的水位
 * @Author         jichangyu
 * @CreateDate     2019/11/28 18:50
 * 周期性的（一定时间间隔或者达到一定的记录条数）产生一个Watermark。在实际的生产中定期的方式必须结合时间和积累条数两个维度继续周期性产生Watermark，否则在极端情况下会有很大的延时。
 * 所以水印的生成方式需要根据业务场景的不同进行不同的选择。
 * 一般情况下使用周期性水印
 */
public class BoundedOutOfOrdernessMsgWaterEmitterPeriodic extends BoundedOutOfOrdernessTimestampExtractor<String> {


    public BoundedOutOfOrdernessMsgWaterEmitterPeriodic(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(String element) {
        long time = 0L;
        if(element != null && element.contains(",")){
            //String[] split = element.split(",");
            //return Long.parseLong(split[0]);

            JSONObject jsonObject = JSON.parseObject(element);
            KafkaConsumerTest consumerTest = JSON.toJavaObject(jsonObject, KafkaConsumerTest.class);
            SimpleDateFormat sdfLong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                 time = sdfLong.parse(consumerTest.getMassage_timestamp()).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return time;
    }
}
