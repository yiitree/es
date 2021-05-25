package com.es.es;

import com.alibaba.fastjson.JSON;
import com.es.es.domain.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient") // 指定方法名，如果client和方法名不一致，就使用此注解
    private RestHighLevelClient client;

    // ----------------------索引操作--------------------------
    /**
     * 创建索引
     * put zr_index
     */
    @Test
    void test01() throws IOException {
        // 创建索引请求
        final CreateIndexRequest request = new CreateIndexRequest("zr_index");
        // 执行请求  --- 请求request，请求参数，使用默认即可
        final CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    /**
     * 获取索引是否存在
     */
    @Test
    void test02() throws IOException {
        final GetIndexRequest request = new GetIndexRequest("zr_index");
        final boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引
     */
    @Test
    void test03() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("zr_index");
        final AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    // ----------------------文档操作--------------------------
    /**
     * 添加文档
     * put /zr_index/_doc/1
     */
    @Test
    void test04() throws IOException {
        final User user = new User("啊啊累", 666);
        // 创建请求 --- 要插入哪个索引库
        final IndexRequest request = new IndexRequest("zr_index");

        // 创建规则 put /zr_index/_doc/1
        // 设置id=1
        request.id("1");
        // 设置过期时间1s
        request.timeout(TimeValue.timeValueSeconds(1));

        // 将数据放到请求 --- 传入json，说明传入类型
        request.source(JSON.toJSONString(user), XContentType.JSON);

        // 客户端发送请求
        final IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status()); // 对应命令的状态，CREATED，如果是更新，就返回UPDATE
    }

    /**
     * 判断文档是否存在
     * get /zr_index/_doc/1
     */
    @Test
    void test05() throws IOException {
        final GetRequest request = new GetRequest("zr_index", "1");

        // 设置不获取的字段FetchSourceContext: _source 的上下文（分组），效率更高
        request.fetchSourceContext(new FetchSourceContext(false));

        // 排序字段
        request.storedFields("_none_");

        // 判断是否存在
        final boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 获取内容
     * get /zr_index/_doc/1
     */
    @Test
    void test06() throws IOException {
        GetRequest request = new GetRequest("zr_index", "1");

        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        System.out.println(getResponse); // 没有转化之前，获取内容和文档信息一样的
        // 获取内容并转化为字符串
        final String sourceAsString = getResponse.getSourceAsString();
        System.out.println(sourceAsString); // 获得数据，并且转化为字符串

    }

    /**
     * 更新文档
     * get /zr_index/_doc/1
     */
    @Test
    void test07() throws IOException {
        UpdateRequest request = new UpdateRequest("zr_index", "1");

        request.timeout("1s");
        final User user = new User("啦啦啦", 18);
        request.doc(JSON.toJSONString(user),XContentType.JSON);
        // 判断是否存在
        final UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response);
        System.out.println(response.status());
    }

    /**
     * 获取内容
     * get /zr_index/_doc/1
     */
    @Test
    void test08() throws IOException {
        DeleteRequest request = new DeleteRequest("zr_index", "2");
        request.timeout("1s");
        final DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status()); // NOT_FOUND
    }

    /**
     * 批量操作 --- 本质就是for循环
     */
    @Test
    void test09() throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout("10s");

        ArrayList<User> list = new ArrayList<>();
        list.add(new User("sss", 10));
        list.add(new User("sss", 10));
        list.add(new User("sss", 10));

        // 批量插入
        for (int i = 0; i < list.size(); i++) {
            request.add(
                    // 插入，如果不写id就随机生成id
                    new IndexRequest("zr_index").id("" + (i + 1)).source(JSON.toJSONString(list.get(i)), XContentType.JSON)
            );
        }
        final BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
        System.out.println(response.hasFailures()); // 是否失败

    }

    /**
     * 查询
     */
    @Test
    void test10() throws IOException {
        SearchRequest request = new SearchRequest("zr_index");
        // 构建搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 使用QueryBuilders工具类构建查询条件
        // termQuery() 精确匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "sss");
        // matchAllQuery() 查询匹配所有
//        QueryBuilders.matchAllQuery();

        // 放到构建器里
        searchSourceBuilder.query(termQueryBuilder);

        // 分页 --- 其他很多都是有默认参数
//        searchSourceBuilder.from();

        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 把搜索条件添加到request中
        request.source(searchSourceBuilder);
        // 进行查询
        final SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 查询结果
        final SearchHits hits = searchResponse.getHits();
        System.out.println(JSON.toJSONString(hits));
        System.out.println("=========================");

        for(SearchHit documentFields : hits.getHits()){
            final Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }
}

