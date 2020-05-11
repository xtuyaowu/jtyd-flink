import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;

/**
 * @author ：jichangyu
 * @date ：Created in 2020/4/26 9:13
 * @description：redistest
 * @modified By：
 * @version: 1.0
 */

public class RedisTest {

    @Test
    public void jedisTest() {
        // 第一步：创建一个Jedis对象。需要指定服务端的ip及端口。
        Jedis jedis = new Jedis("master.northking.com", 6379);
        jedis.auth("123456");
        // 第二步：使用Jedis对象操作数据库，每个redis命令对应一个方法。
        Set<String> jedisSet = jedis.smembers("0030001");
        // 第三步：打印结果。
        System.out.println(jedisSet.size());
        // 第四步：关闭jedis
        jedis.close();
    }

    @Test
    public void jedisPoolTest(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMaxWaitMillis(2000);
        //在获取连接的时候检查有效性, 默认false
        jedisPoolConfig.setTestOnBorrow(false);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig,"master.northking.com", 6379);
        Jedis jedis = jedisPool.getResource();
        jedis.auth("123456");
        //Set<String> jedisSet = jedis.smembers("0030001");
        String name = jedis.hget("0030002", "name");
        System.out.println(name);
        jedis.close();
        jedisPool.close();
    }
}
