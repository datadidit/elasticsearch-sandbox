package org.datadidit.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.*;


public class ElasticPlayIT {
	private String clusterName; 
	
	private static TransportClient client; 

	private static final String HOSTNAME = "127.0.0.1"; 
	
	@BeforeClass
	public static void setupBeforeClass() throws UnknownHostException {
		//Local testing only one node cluster
		//Instructions: https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/transport-client.html
		client = new PreBuiltTransportClient(Settings.EMPTY)
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOSTNAME), 9300));

	}
	
	@Test
	public void testIndexAndGet() {
		String json = "{" +
		        "\"user\":\"kimchy\"," +
		        "\"postDate\":\"2013-01-30\"," +
		        "\"message\":\"trying out Elasticsearch\"," +
		        "\"postNumber\":\"10\""+
		    "}";
		
		//Index Data
		IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
		        .setSource(json)
		        .get();
		
		//GET Indexed Data 
		GetResponse getResponse = client.prepareGet("twitter", "tweet", "1").get();
		System.out.println(getResponse);		
	}
	
	@Test
	public void testSearchData() {
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-search.html
		SearchResponse response = client.prepareSearch("twitter")
				.setTypes("tweet")
				.setSearchType(SearchType.DEFAULT)
				.setQuery(QueryBuilders.termQuery("user", "kimchy"))
				.get();
		
		for(SearchHit hit : response.getHits()) {
			System.out.println(hit.getSourceAsMap());
		}
	}
	
	@Test
	public void testQueryDSL() {
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-query-dsl.html
		QueryBuilder qb = matchAllQuery();

		SearchResponse response = client.prepareSearch("twitter")
				.setTypes("tweet")
				.setSearchType(SearchType.DEFAULT)
				.setQuery(matchAllQuery())
				.get();
		
		for(SearchHit hit : response.getHits()) {
			System.out.println(hit.getSourceAsMap());
		}
	}
	
	@Test
	public void testAggregation() {
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-aggs.html
		//TODO: Figure this out???
		SumAggregationBuilder aggregation =
		        AggregationBuilders
		                .sum("agg")
		                .field("postNumber");
		
		SearchResponse response = client.prepareSearch("twitter")
				.setTypes("tweet")
				.setSearchType(SearchType.DEFAULT)
				.setQuery(matchAllQuery())
				.addAggregation(aggregation)
				.execute().actionGet();
		
		System.out.println(response);
	}
	
	@AfterClass
	public static void shutdown() {
		// on shutdown
		client.close();		
	}
}
