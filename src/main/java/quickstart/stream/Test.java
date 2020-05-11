package quickstart.stream;

import com.alibaba.fastjson.JSON;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Test {
    public static void main(String[] args) throws Exception {

        String s = "{\"NAME\":\"zhangsan\",\"HEARTBEAT\":\"100\"}";

        System.out.println(JSON.parseObject(s).toString());
//        System.out.println(System.currentTimeMillis());
//
//        SimpleDateFormat sdfLong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        System.out.println(sdfLong.parse("2019-06-01 13:20:18").getTime());
//        System.out.println(String.format("%d", System.currentTimeMillis()));
    }
}
