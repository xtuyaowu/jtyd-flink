package util;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ：jichangyu
 * @date ：Created in 2020/4/26 15:06
 * @description：ElasticSearch7Util
 * @modified By：
 * @version: 1.0
 */
public class ElasticSearch7Util {

//    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch7Util.class);
//
//    /**
//     * 构建查询构造器
//     *
//     * @param indexName    索引名
//     * @param whereExpress 查询条件:(f1=2 and f2=1) or (f3=1 and f4=1)
//     * @return
//     */
//    public static BoolQueryBuilder createQueryBuilderByWhere(String indexName, String whereExpress) {
////        BoolQueryBuilder boolQuery = null;
////
////        try {
////            String sql = "select * from " + indexName;
////            String whereTemp = "";
////            if (StringUtils.isNotBlank(whereExpress)) {
////                whereTemp = " where 1=1 " + whereExpress;
////            }
////            SQLQueryExpr sqlExpr = (SQLQueryExpr) toSqlExpr(sql + whereTemp);
////            SqlParser sqlParser = new SqlParser();
////            MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlExpr.getSubQuery().getQuery();
////            WhereParser whereParser = new WhereParser(sqlParser, query);
////            Where where = whereParser.findWhere();
////            if (where != null) {
////                boolQuery = QueryMaker.explan(where);
////            }
////        } catch (SqlParseException e) {
////            logger.info("ReadES.createQueryBuilderByExpress-Exception," + e.getMessage());
////            e.printStackTrace();
////        }
////        return boolQuery;
//    }
//
//    /**
//     * 验证sql
//     *
//     * @param sql sql查询语句
//     * @return and (a=1 and b=1) or (c=1 and d=1)
//     */
//    private static SQLExpr toSqlExpr(String sql) {
////        SQLExprParser parser = new ElasticSqlExprParser(sql);
////        SQLExpr expr = parser.expr();
////
////        if (parser.getLexer().token() != Token.EOF) {
////            throw new ParserException("illegal sql expr : " + sql);
////        }
////        return expr;
//    }
//
//    /**
//     * 查询指定索引下的信息
//     *
//     * @param indexName 索引名称
//     * @param condition 查询条件
//     */
//    public static List<Map<String, Object>> queryIndexContent(String indexName, String condition) throws IOException {
//        //实例化查询请求对象
//        SearchRequest request = new SearchRequest();
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
//
//        //实例化SearchSourceBuilder
//        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
//        //根据索引、查询条件构建查询构造器
//        BoolQueryBuilder boolQueryBuilder = createQueryBuilderByWhere(indexName, condition);
//        //将查询构造器注入SearchSourceBuilder
//        searchBuilder.query(boolQueryBuilder);
//        //设置请求查询的索引（查询构造器中已指定，无需重复设置）
//        //request.indices(indexName);
//        //将构建好的SearchSourceBuilder注入请求
//        request.source(searchBuilder);
//
//
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
//            logger.info(datas.toString());
//        }
//        //关闭客户端连接
//        client.close();
//
//        return listData;
//    }
}
