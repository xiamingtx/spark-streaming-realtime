package com.xm.gmall.publisherrealtime.mapper.impl;

import com.xm.gmall.publisherrealtime.bean.NameValue;
import com.xm.gmall.publisherrealtime.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 夏明
 * @version 1.0
 */
@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

    @Autowired
    RestHighLevelClient esClient;

    private String dauIndexNamePrefix = "gmall_dau_info_1018_";

    private String orderIndexPrefix = "gmall_order_wide_1018_";

    @Override
    public Map<String, Object> searchDetailByItem(String date, String itemName, Integer from, Integer pageSize) {
        HashMap<String, Object> results = new HashMap<>();
        String indexName = orderIndexPrefix + date;
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 明细字段
        searchSourceBuilder.fetchSource(new String[]{
                "create_time", "order_price", "province_name", "sku_name",
                "sku_num", "total_amount", "user_age", "user_gender"
        }, null);
        // query
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);

        // from
        searchSourceBuilder.from(from);

        // size
        searchSourceBuilder.size(pageSize);

        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("sku_name");
        searchSourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long total = searchResponse.getHits().getTotalHits().value;
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            ArrayList<Map<String, Object>> sourceMaps = new ArrayList<>();

            for (SearchHit searchHit : searchHits) {
                // 提取source
                Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                // 提取高亮
                Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
                HighlightField highlightField = highlightFields.get("sku_name");
                Text[] fragments = highlightField.getFragments();
                String highLightSkuName = fragments[0].toString();
                // 使用高亮结果覆盖原来结果
                sourceMap.put("sku_name", highLightSkuName);
                sourceMaps.add(sourceMap);
            }
            // 最终结果
            results.put("total", total);
            results.put("detail", sourceMaps);

            return results;
        } catch(ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在...");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询es失败");
        }
        return results;
    }

    /**
     *
     * @author 夏明
     * @date 2022/11/6 19:22
     * @param itemName
     * @param date
     * @param field age => user_age gender => user_gender
     * @return List<NameValue>
     */
    @Override
    public List<NameValue> searchStatsByItem(String itemName, String date, String field) {
        ArrayList<NameValue> results = new ArrayList<>();
        String indexName = orderIndexPrefix + date;
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 不需要明细
        searchSourceBuilder.size(0);
        // query
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);
        // group
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupby" + field).field(field).size(100);
        // sum
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("totalamount").field("split_total_amount");
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupby" + field);
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String key = bucket.getKeyAsString();
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedSum parsedSum = bucketAggregations.get("totalamount");
                double totalAmount = parsedSum.getValue();
                results.add(new NameValue(key, totalAmount));
            }
            return results;
        } catch(ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在...");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询es失败");
        }
        return results;
    }

    @Override
    public Map<String, Object> searchDau(String td) {
        Map<String, Object> dauResults = new HashMap<>();
        // 日活总数
        Long dauTotal = searchDauTotal(td);
        dauResults.put("dauTotal", dauTotal);

        // 今日分时明细
        Map<String, Long> dauTd = searchDauHr(td);
        dauResults.put("dauTd", dauTd);

        // 今日分时明细
        LocalDate tdLd = LocalDate.parse(td);
        LocalDate ydLd = tdLd.minusDays(1);

        Map<String, Long> dauYd = searchDauHr(ydLd.toString());
        dauResults.put("dauYd", dauYd);
        return dauResults;
    }

    public Map<String, Long> searchDauHr(String td) {
        HashMap<String, Long> dauHr = new HashMap<>();
        String indexName = dauIndexNamePrefix + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 不要明细
        searchSourceBuilder.size(0);
        // 聚合
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupbyhr").field("hr").size(24);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupbyhr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String hr = bucket.getKeyAsString();
                long hrTotal = bucket.getDocCount();
                dauHr.put(hr, hrTotal);
            }
            return dauHr;
        } catch(ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在...");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询es失败");
        }
        return dauHr;
    }

    public Long searchDauTotal(String td) {
        String indexName = dauIndexNamePrefix + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 不要明细
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long dauTotal = searchResponse.getHits().getTotalHits().value;
            return dauTotal;
        } catch(ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在...");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询es失败");
        }
        return 0L;
    }
}
