package org.datadidit.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.elasticsearch5.ElasticsearchEndpoint;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.datadidit.camel.GeoEnrichmentProcessor;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ElasticSearchCamelIT extends CamelTestSupport {
	@Produce(uri = "direct:start")
	protected ProducerTemplate template;

	@EndpointInject(uri = "elasticsearch5://elasticsearch?indexName=twitter&indexType=tweet&operation=BULK_INDEX&ip=127.0.0.1&port=9300")
	protected ElasticsearchEndpoint elastic;
	
	@EndpointInject(uri = "mock:result")
	protected MockEndpoint resultEndpoint;
	
	private static String apiKey;
	
	private ObjectMapper mapper = new ObjectMapper(); 

	@BeforeClass
	public static void setupGeo(){
		apiKey = System.getenv("apiKey");
		if(apiKey==null)
			fail("Unable to get Api Key set up environment variable for this test!");
		else
			System.out.println("API Key is "+apiKey);
	}
	
	@Test
	public void testGeoToElastic() throws JsonProcessingException, InterruptedException {
		String[] mdLocations = new String[] {"Forestville", "Largo", "Poolesville", "Annapolis"};
		List<Map<String, Object>> incomingData = new ArrayList<>();
		
		for(String city : mdLocations) {
			Map<String, Object> geoInfo = new HashMap<>();

			geoInfo.put("State", "MD");
			geoInfo.put("City", city);
			geoInfo.put("Country", "US");
			
			incomingData.add(geoInfo);
		}
		
		String input = mapper.writeValueAsString(incomingData);

		resultEndpoint.expectedMinimumMessageCount(1);
		template.sendBody(input);
		resultEndpoint.assertIsSatisfied();
	}
	
	/*
	 * Create Camel Route for Processor 
	 */
	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			public void configure() {
				GeoEnrichmentProcessor processor = new GeoEnrichmentProcessor(apiKey, "City,State,Country", "geometry");
				
				from("direct:start")
				 .process(processor)
				 	.to(elastic)
				 		.to(resultEndpoint);
			}
		};
	}
}
