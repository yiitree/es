package com.es.es.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Es的配置文件
 */
@Configuration
public class EsClientConfig {

    @Bean
    public RestHighLevelClient restHighLevelClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("mnrui.top", 9200, "http")
//                        new HttpHost("localhost", 9201, "http")) // 如果是集群就配置多个
        ));
        return client;
    }

    public static void main(String[] args) {
        System.out.println("1111");
    }

}
