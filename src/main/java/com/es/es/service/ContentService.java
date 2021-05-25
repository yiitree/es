package com.es.es.service;

import com.alibaba.fastjson.JSON;
import com.es.es.domain.Content;
import com.es.es.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class ContentService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 解析数据到 es 索引中
     * @param keywords 搜索关键字
     * @return
     */
    public Boolean parseContent(String keywords) throws IOException {
        List<Content> contents = new HtmlParseUtil().parseJd(keywords);
        final BulkRequest request = new BulkRequest();
        request.timeout("2m");

        for (int i = 0; i < contents.size(); i++) {
            request.add(new IndexRequest("jd_goods").source(JSON.toJSONString(contents.get(i)), XContentType.JSON));
        }
        final BulkResponse response = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        return !response.hasFailures();
    }

    /**
     * 搜索 es 中的数据
     * @param keywords 搜索关键字
     * @return
     */
    public List<Map<String,Object>> searchPage(int pageNo,int pageSize,String keywords) throws IOException {
        if(pageNo <= 1){
            pageNo = 1;
        }

        // 条件搜索
        SearchRequest request = new SearchRequest("jd_goods");
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        // 精确匹配
        final TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keywords);
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 执行搜索
        request.source(searchSourceBuilder);
        final SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        // 解析结果
        ArrayList<Map<String,Object>> list = new ArrayList<>();
        for (SearchHit documentFields : response.getHits().getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    /**
     * 搜索 es 中的数据
     * @param keywords 搜索关键字
     * @return
     */
    public List<Map<String,Object>> searchPageGl(int pageNo,int pageSize,String keywords) throws IOException {
        if(pageNo <= 1){
            pageNo = 1;
        }

        // 条件搜索
        SearchRequest request = new SearchRequest("jd_goods");
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        // 精确匹配
        final TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keywords);
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 高亮搜索
        final HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        // 是否需要多个高亮
        highlightBuilder.requireFieldMatch(false);
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        searchSourceBuilder.highlighter(highlightBuilder);

        // 执行搜索
        request.source(searchSourceBuilder);
        final SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        // 解析结果
        ArrayList<Map<String,Object>> list = new ArrayList<>();
        for (SearchHit documentFields : response.getHits().getHits()) {
            // 解析高亮字段
            final Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
            final HighlightField title = highlightFields.get("title");
            final Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();// 原来的数据


            // 如果有高亮字段
            if(title != null){
                // 在原来的字段进行置换
                final Text[] fragments = title.fragments();
                String newTitle = "";
                for (Text fragment : fragments) {
                    newTitle += fragment;
                }
                sourceAsMap.put("title", newTitle);// 把高亮字段替换原来的字段
            }

            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }
}
