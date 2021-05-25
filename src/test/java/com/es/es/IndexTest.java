package com.es.es;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * 索引操作
 */
@SpringBootTest
class IndexTest {

    @Autowired
    @Qualifier("restHighLevelClient") // 指定方法名，如果client和方法名不一致，就使用此注解
    private RestHighLevelClient client;

    /**
     * 创建索引
     * CreateIndexRequest
     * put zr_index
     */
    @Test
    void test01() throws IOException {
        // 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("zr_index");
        // 执行请求  --- 请求request，请求参数，使用默认即可
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    /**
     * 获取索引是否存在
     * GetIndexRequest
     */
    @Test
    void test02() throws IOException {
        GetIndexRequest request = new GetIndexRequest("zr_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引
     * DeleteIndexRequest
     */
    @Test
    void test03() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("zr_index");
        final AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }
}
