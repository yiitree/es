package com.es.es;

import com.alibaba.fastjson.JSON;
import com.es.es.domain.User;
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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 文档操作
 */
@SpringBootTest
public class DocTest {

    @Autowired
    @Qualifier("restHighLevelClient") // 指定方法名，如果client和方法名不一致，就使用此注解
    private RestHighLevelClient client;

    /**
     * 添加文档
     * IndexRequest
     * put /zr_index/_doc/1
     */
    @Test
    public void test04() throws IOException {
        // 创建请求 --- 要插入哪个索引库
        IndexRequest request = new IndexRequest("zr_index");

        // 创建规则 put /zr_index/_doc/1
        // 设置id=1
        request.id("1");
        // 设置过期时间1s
        request.timeout(TimeValue.timeValueSeconds(1));

        // 将数据放到请求 --- 传入json，说明传入类型
        request.source(JSON.toJSONString(new User("啊啊累", 666)), XContentType.JSON);

        // 客户端发送请求
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status()); // 对应命令的状态，CREATED，如果是更新，就返回UPDATE
    }

    /**
     * 删除文档
     * DeleteRequest
     */
    @Test
    void delete() throws IOException {
        final DeleteRequest request = new DeleteRequest("zr_index", "2");
        request.timeout("1s");
        final DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status()); // NOT_FOUND
    }

    /**
     * 更新文档
     * UpdateRequest
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
     * 查询 判断文档是否存在
     * GetRequest
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
     * 查询 获取内容
     * GetRequest
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
     * 批量操作 --- 本质就是for循环
     * BulkRequest
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
     * 搜索
     * SearchRequest
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

    /**
     * 高亮搜索 + 排序
     */
    @Test
    public void test11()throws IOException{
        int pageNo = 1;
        int pageSize = 1;
        String keywords = "java";

        if(pageNo <= 1){
            pageNo = 1;
        }

        SearchRequest request = new SearchRequest("jd_goods");

        // 条件搜索
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 1 分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        // 2 精确匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keywords);
        searchSourceBuilder.query(termQueryBuilder);

        // 3 超时时间
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 4 高亮搜索
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        // 是否需要多个高亮
        highlightBuilder.requireFieldMatch(false);
        // 前缀后缀
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        searchSourceBuilder.highlighter(highlightBuilder);

        // 执行搜索
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 解析结果
        ArrayList<Map<String,Object>> list = new ArrayList<>();
        for (SearchHit documentFields : response.getHits().getHits()) {
            // 原本数据
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();

            // 高亮字段 --- 一条数据里有可能有多个高亮字段，例如：java书籍之java高级编程
            Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
            HighlightField highlightField = highlightFields.get("title");

            // 如果有高亮字段
            if(highlightField != null){
                // 在原来的字段进行置换
                Text[] fragments = highlightField.fragments();
                StringBuilder newTitle = new StringBuilder();
                for (Text fragment : fragments) {
                    newTitle.append(fragment);
                }
                sourceAsMap.put("title", newTitle.toString());// 把高亮字段替换原来的字段
            }

            list.add(documentFields.getSourceAsMap());
        }
        System.out.println(list);
    }

}
