package com.xm.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.text.Text
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilders, RangeQueryBuilder, TermQueryBuilder}
import org.elasticsearch.index.reindex.UpdateByQueryRequest
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.{Aggregation, AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.aggregations.bucket.terms.{ParsedTerms, Terms, TermsAggregationBuilder}
import org.elasticsearch.search.aggregations.metrics.{AvgAggregationBuilder, ParsedAvg}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder

import java.util

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
/*
  ES客户端
 */
object EsTest {
  def main(args: Array[String]): Unit = {
    // println(client)
    // put()
    // post()
    // bulk()
    // update()
    // updateByQuery()
    // delete()
    searchById()
    close()
  }

  /**
   * 查询单条查询
   */
  def searchById(): Unit = {
    val getRequest: GetRequest = new GetRequest("movie_test", "1001")
    val getResponse: GetResponse = client.get(getRequest, RequestOptions.DEFAULT)
    val dataStr: String = getResponse.getSourceAsString
    println(dataStr)
  }

  /**
   * 查询-条件查询
   * search :
   * 查询doubanScore>=5.0关 键词搜索red sea
   * 关键词高亮显示
   * 显示第一页，每页2条
   * 按doubanScore从大到小排序
   */
  def searchByFilter(): Unit = {
    val searchRequest: SearchRequest = new SearchRequest("movie_index")
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    // query
    // bool
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    // filter
    val rangeQueryBuilder: RangeQueryBuilder = QueryBuilders.rangeQuery("doubanScore").gte(5.0)
    boolQueryBuilder.filter(rangeQueryBuilder)
    // must
    val matchQueryBuilder: MatchQueryBuilder = QueryBuilders.matchQuery("name", "red sea")
    boolQueryBuilder.must(matchQueryBuilder)
    searchSourceBuilder.query(boolQueryBuilder)

    // 分页
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(2)

    // 排序
    searchSourceBuilder.sort("doubanScore", SortOrder.DESC)

    // 高亮
    val highlightBuilder: HighlightBuilder = new HighlightBuilder()
    highlightBuilder.field("name")
    searchSourceBuilder.highlighter(highlightBuilder)

    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

    // 获取总条数据
    val totalDocs: Long = searchResponse.getHits.getTotalHits.value

    // 明细
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      // 数据
      val dataJson: String = hit.getSourceAsString
      // 提取高亮
      val highLightFields: util.Map[String, HighlightField] = hit.getHighlightFields
      val highLightField: HighlightField = highLightFields.get("name")
      val fragments: Array[Text] = highLightField.getFragments
      val highLightValue: String = fragments(0).toString

      println("明细数据: " + dataJson)
      println("高亮: " + highLightValue)
    }
  }

  /**
   * 查询-聚合查询
   *
   * 查询每位演员参演电影的平均分 倒序排序
   */
  def searchAggs(): Unit = {
    val searchRequest: SearchRequest = new SearchRequest("movie_index")
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    // 不要明细
    searchSourceBuilder.size(0)
    // group
    val termsAggregationBuilder: TermsAggregationBuilder =
      AggregationBuilders.terms("groupbyactorname")
        .field("actorList.name.keyword")
        .size(10)
        .order(BucketOrder.aggregation("doubanscoreavg", false))
    // avg
    val avgAggregationBuilder: AvgAggregationBuilder = AggregationBuilders.avg("doubanscoreavg").field("doubanScore")
    termsAggregationBuilder.subAggregation(avgAggregationBuilder)
    searchSourceBuilder.aggregation(termsAggregationBuilder)

    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

    val aggregations: Aggregations = searchResponse.getAggregations
    val groupbyactornameParsedTerms: ParsedTerms = aggregations.get[ParsedTerms]("groupbyactorname")
    val buckets: util.List[_ <: Terms.Bucket] = groupbyactornameParsedTerms.getBuckets
    import scala.collection.JavaConverters._
    for (bucket <- buckets.asScala) {
      // 演员名字
      val actorName: String = bucket.getKeyAsString
      // 电影个数
      val movieCount: Long = bucket.getDocCount
      // 平均分
      val aggregations: Aggregations = bucket.getAggregations
      val doubanscoreavgParsedAvg: ParsedAvg = aggregations.get[ParsedAvg]("doubanscoreavg")
      val avgScore: Double = doubanscoreavgParsedAvg.getValue

      println(s"$actorName 共参演了 $movieCount 部电影, 平均分为 $avgScore")
    }
  }

  /**
   * 删除
   */
  def delete(): Unit = {
    val deleteRequest: DeleteRequest = new DeleteRequest("movie_test", "abvdQYQBctMRibz7pYER")
    client.delete(deleteRequest, RequestOptions.DEFAULT)
  }

  /**
   * 修改-单条修改
   */
  def update(): Unit = {
    val updateRequest: UpdateRequest = new UpdateRequest("movie_test", "1001")
    updateRequest.doc("movie_name", "功夫")
    client.update(updateRequest, RequestOptions.DEFAULT)
  }

  /**
   * 修改-条件修改
   */
  def updateByQuery(): Unit = {
    val updateByQueryRequest: UpdateByQueryRequest = new UpdateByQueryRequest("movie_test")
    // query
    // val termQueryBuilder: TermQueryBuilder = new TermQueryBuilder("movie_name.keyword", "红海行动")
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    val termQueryBuilder: TermQueryBuilder = QueryBuilders.termQuery("movie_name.keyword", "速度与激情")
    boolQueryBuilder.filter(termQueryBuilder)
    updateByQueryRequest.setQuery(boolQueryBuilder)
    // update
    val params: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
    params.put("newName", "湄公河行动")

    val script: Script = new Script(
      ScriptType.INLINE,
      Script.DEFAULT_SCRIPT_LANG,
      "ctx.source['movie_name']=params.newName",
      params
    )
    updateByQueryRequest.setScript(script)
    client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT)
  }

  /**
   * 批量写
   */
  def bulk(): Unit = {
    val bulkRequest: BulkRequest = new BulkRequest()
    val movies: List[Movie] = List[Movie](
      Movie("1002", "长津湖"),
      Movie("1003", "水门桥"),
      Movie("1004", "狙击手"),
      Movie("1005", "熊出没")
    )
    for (movie <- movies) {
      val indexRequest: IndexRequest = new IndexRequest("movie_test") // 指定索引
      val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
      indexRequest.source(movieJson, XContentType.JSON)
      // 幂等写指定id 非幂等写不指定id
      indexRequest.id(movie.id)
      // 将indexRequest加入bulk中
      bulkRequest.add(indexRequest)
    }
    client.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   * 增-幂等-指定doc_id
   */
  def put(): Unit = {
    val indexRequest: IndexRequest = new IndexRequest()
    // 指定索引 也可以在构造方法中指定
    indexRequest.index("movie_test")
    // 指定doc
    val movie: Movie = Movie("1001", "速度与激情")
    val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(movieJson, XContentType.JSON)
    // 指定doc_id
    indexRequest.id("1001")
    // 执行
    client.index(indexRequest, RequestOptions.DEFAULT)
  }
  /**
   * 增-非幂等-不指定doc_id
   */
  def post(): Unit = {
    val indexRequest: IndexRequest = new IndexRequest()
    // 指定索引 也可以在构造方法中指定
    indexRequest.index("movie_test")
    // 指定doc
    val movie: Movie = Movie("1001", "速度与激情")
    val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(movieJson, XContentType.JSON)
    // 执行
    client.index(indexRequest, RequestOptions.DEFAULT)
  }


  /* 客户端对象 */
  var client: RestHighLevelClient = create()

  // 创建客户端对象
  def create(): RestHighLevelClient = {
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost("mytencent", 9200))
    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    client
  }

  // 关闭客户端对象
  def close(): Unit = {
    if (client != null) client.close()
  }
}

case class Movie(id: String, movie_name: String)
