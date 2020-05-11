package quickstart.stream.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickstart.stream.function.AsyncFunctionForMysqlJava;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author ：jichangyu
 * @date ：Created in 2020/4/24 17:14
 * @description：异步关联redis数据
 * @modified By：
 * @version: 1.0
 */
public class AsyncFunctionRedisRequest extends RichAsyncFunction<String, String> {
    // 创建日志
    Logger logger = LoggerFactory.getLogger(AsyncFunctionForMysqlJava.class);
    JedisPool jedisPool = null;
    Jedis jedis = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger.info("async function for redis java open ...");
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMaxWaitMillis(2000);
        //在获取连接的时候检查有效性, 默认false
        jedisPoolConfig.setTestOnBorrow(false);
        jedisPool = new JedisPool(jedisPoolConfig, "master.northking.com", 6379);
        jedis = jedisPool.getResource();
        jedis.auth("123456");
        System.out.println("获得jedis:"+jedis);
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        JSONObject jsonObject = JSON.parseObject(input);
        String id = jsonObject.getString("id");
        String name = jedis.hget(id, "name");
        jsonObject.put("name", name);
        resultFuture.complete(Collections.singleton(jsonObject.toString()));
    }

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
        logger.warn("Async function for redis timeout");
        List<String> list = new ArrayList();
        JSONObject jsonObject = JSON.parseObject(input);
        String result = jsonObject.put("name", "").toString();
        list.add(result);
        resultFuture.complete(list);
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
