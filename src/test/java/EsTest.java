import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WrapperQueryBuilder;
//import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.query.QueryAction;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//import static util.ElasticSearch7Util.queryIndexContent;

/**
 * @author ：jichangyu
 * @date ：Created in 2020/4/26 15:06
 * @description：es操作测试
 * @modified By：
 * @version: 1.0
 */
public class EsTest {

    @Test
    public void queryTest(){
//        try {
//            List<Map<String, Object>> t_s_s_jylsjh = queryIndexContent("t_s_s_jylsjh", " and hxjylsh ='1001004' and dgdsbs='01' ");
//            System.out.println("查询结果:"+t_s_s_jylsjh.toString());
//        } catch(Exception ex) {
//            ex.printStackTrace();
//        }
    }

    @Test
    public void  httpQueryTest() throws IOException {
//        //构建查询客户端
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials("elastic", "123456"));//es账号密码
//        RestClientBuilder builder = RestClient.builder(new HttpHost("master.northking.com", 9200, "http")).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
//            @Override
//            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
//                requestConfigBuilder.setConnectTimeout(-1);
//                requestConfigBuilder.setSocketTimeout(-1);
//                requestConfigBuilder.setConnectionRequestTimeout(-1);
//                return requestConfigBuilder;
//            }
//        }).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//            @Override
//            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                httpClientBuilder.disableAuthCaching();
//                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//            }
//        });//初始化
//        RestHighLevelClient client = new RestHighLevelClient(builder);
//        StringBuffer dsl = new StringBuffer();
//        dsl.append("_sql?format=txt");
//        dsl.append("{");
//        dsl.append("  \"query\":\"select count(*) from t_s_s_ckzhjh\"");
//        dsl.append("}");
//
//        WrapperQueryBuilder wqp = QueryBuilders.wrapperQuery(dsl.toString());
//        //实例化查询请求对象
//        SearchRequest request = new SearchRequest();
//        //实例化SearchSourceBuilder
//        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
//        searchBuilder.query(wqp);
//        request.source(searchBuilder);
//        //带入请求执行查询
//        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
//
//        //得到查询结果
//        SearchHits hits = searchResponse.getHits();
//
//        SearchHit[] searchHits = hits.getHits();
//
//        List<Map<String, Object>> listData = new ArrayList<>();
//        //遍历查询结果
//        for (SearchHit hit : searchHits) {
//            Map<String, Object> datas = hit.getSourceAsMap();
//            listData.add(datas);
//        }
//        //关闭客户端连接
//        client.close();

    }

    @Test
    public void  sqlQueryTest() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "bank")
//                .put("xpack.security.user", "elastic:123456")
                .build();
        // 创建客户端，如果使用默认配置，传参为 Settings.Empty

        TransportClient client = new PreBuiltTransportClient(settings)
           .addTransportAddress(new TransportAddress(InetAddress.getByName("master.northking.com"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("slave1.northking.com"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("slave2.northking.com"), 9300));



        System.out.println(client.toString());

        //1.解释SQL
        SearchDao searchDao = new SearchDao(client);
//        String sql="";
        QueryAction queryAction = searchDao.explain("select count(*) from t_s_s_ckzhjh");
        System.out.println(queryAction.toString());
        //2.执行

                //构建查询客户端
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials("elastic", "123456"));//es账号密码
        RestClientBuilder builder = RestClient.builder(new HttpHost("master.northking.com", 9200, "http")).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                requestConfigBuilder.setConnectTimeout(-1);
                requestConfigBuilder.setSocketTimeout(-1);
                requestConfigBuilder.setConnectionRequestTimeout(-1);
                return requestConfigBuilder;
            }
        }).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });//初始化
        RestHighLevelClient client1 = new RestHighLevelClient(builder);

        Object execution = QueryActionElasticExecutor.executeAnyAction((Client) client1, queryAction);
        //Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        //3.格式化查询结果
        ObjectResult result = (new ObjectResultsExtractor(true, false, false)).extractResults(execution, true);
        System.out.println(result.toString());
    }

}
