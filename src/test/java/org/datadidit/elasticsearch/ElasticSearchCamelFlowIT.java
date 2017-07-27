package org.datadidit.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.direct.DirectEndpoint;
import org.apache.camel.component.elasticsearch5.ElasticsearchEndpoint;
import org.apache.camel.component.file.FileEndpoint;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultProducerTemplate;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.commons.io.FileUtils;
import org.datadidit.camel.GeoEnrichmentProcessor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import datadidit.helpful.hints.camel.CSVToJsonProcessor;

public class ElasticSearchCamelFlowIT extends CamelTestSupport{
	private static final String INDEX_NAME = "test";
	
	private static final String INDEX_TYPE = "baseball";
	
	private static final String HOSTNAME = "127.0.0.1";
	
	private static final String BASEBALLDIR = "src/test/resources/test-flow/baseball";
	
	private static final String FOOTBALLDIR = "src/test/resources/test-flow/football";
	
	private static final String HOCKEYDIR = "src/test/resources/test-flow/hockey";
	
	private static final String BASKETBALLDIR = "src/test/resources/test-flow/basketball";
	
	private static final String BACKUPENRICHEDDATA = "src/test/resources/test-flow/backup";
	
	@EndpointInject(uri = "file://"+BASEBALLDIR)
	protected FileEndpoint baseballFileEndpoint;
	
	@EndpointInject(uri = "file://"+FOOTBALLDIR)
	protected FileEndpoint footballFileEndpoint;
	
	@EndpointInject(uri = "file://"+HOCKEYDIR)
	protected FileEndpoint hockeyFileEndpoint;
	
	@EndpointInject(uri = "file://"+BASKETBALLDIR)
	protected FileEndpoint basketballFileEndpoint;
	
	@EndpointInject(uri = "file://"+BACKUPENRICHEDDATA)
	protected FileEndpoint backupFileEndpoint;
	
	@EndpointInject(uri = "direct:georouter")
	protected DirectEndpoint directGeoRouter;
	
	@EndpointInject(uri = "direct:baseballgeo")
	protected DirectEndpoint directToBaseballGeoEndpoint;
	
	@EndpointInject(uri = "direct:footballgeo")
	protected DirectEndpoint directToFootballGeoEndpoint;

	@EndpointInject(uri = "direct:hockeygeo")
	protected DirectEndpoint directToHockeyGeoEndpoint;
	
	@EndpointInject(uri = "direct:basketballgeo")
	protected DirectEndpoint directToBasketballGeoEndpoint;
	
	@EndpointInject(uri = "elasticsearch5://elasticsearch?indexName="+INDEX_NAME+"&indexType="+INDEX_TYPE+"&operation=BULK_INDEX&ip=127.0.0.1&port=9300")
	protected ElasticsearchEndpoint elastic;
	
	@EndpointInject(uri = "mock:result")
	protected MockEndpoint result;
	
	private static String apiKey; 
	
	@BeforeClass
	public static void setupGeo() throws UnknownHostException{
		apiKey = System.getenv("apiKey");
		if(apiKey==null)
			fail("Unable to get Api Key set up environment variable for this test!");
		else
			System.out.println("API Key is "+apiKey);
	}
	
	@Test
	public void testFlow() throws InterruptedException {
		Integer expectedCount = 1;
		Integer waitTime = 30;
		result.expectedMinimumMessageCount(expectedCount);
		
		/*
		 * Route has 30 seconds to complete
		 */
		Integer i=0;
		while(result.getReceivedCounter()<expectedCount && i<waitTime) {
			Thread.sleep(1000);
			i++;
			System.out.println("Message count "+result.getReceivedCounter()+" Number of secs: "+i);
		}
		
		result.assertIsSatisfied();
	}
	
	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			public void configure() {
				CSVToJsonProcessor csvProcessor = null;
				GeoEnrichmentProcessor baseballGeoProcessor = null, footballAndBasketballGeoProcessor = null, hockeyGeoProcessor = null;
				
				try {
					csvProcessor = new CSVToJsonProcessor(true, null);
					
					String geoKey = "geometry";
					baseballGeoProcessor = new GeoEnrichmentProcessor(apiKey, "Birthplace,State,Country", geoKey);
					footballAndBasketballGeoProcessor = new GeoEnrichmentProcessor(apiKey, "City,State,Country", geoKey);
					hockeyGeoProcessor = new GeoEnrichmentProcessor(apiKey, "Birth City,Province,State,Country", geoKey);
					
					//Need to set default Endpoint
					DefaultProducerTemplate template = new DefaultProducerTemplate(this.getContext(), directGeoRouter);
					template.start();
					csvProcessor.setProducer(template);
				} catch (Exception e) {
					e.printStackTrace();
					fail("Unable to create processor "+e.getMessage());
				}
				
				/*
				 * Turn CSV to JSON
				 */
				//from(baseballFileEndpoint)
				//.setHeader("type", constant("baseball"))
				//.process(csvProcessor);
				
				//from(footballFileEndpoint)
				//.setHeader("type", constant("football"))
				//.process(csvProcessor);
				
				//from(hockeyFileEndpoint)
				//.setHeader("type", constant("hockey"))
				//.process(csvProcessor);
				
				from(basketballFileEndpoint)
				.setHeader("type", constant("basketball"))
				.process(csvProcessor);
				
				/*
				 * Route data 
				 */
				from(directGeoRouter)
					.choice()
						.when(header("type").isEqualTo(constant("baseball")))
							.to(directToBaseballGeoEndpoint)
						.when(header("type").isEqualTo(constant("football")))
							.to(directToFootballGeoEndpoint)
						.when(header("type").isEqualTo(constant("basketball")))
							.to(directToFootballGeoEndpoint)
						.when(header("type").isEqualTo(constant("hockey")))
							.to(directToHockeyGeoEndpoint)
						.otherwise()
							.log("Something isn't right");
						
				
				/*
				 * Geo Enhance
				 */
				from(directToBaseballGeoEndpoint)
				.process(baseballGeoProcessor)
				.to("direct:backupAndElastic");

				from(directToFootballGeoEndpoint)
				.process(footballAndBasketballGeoProcessor)
				.to("direct:backupAndElastic");
				
				from(directToHockeyGeoEndpoint)
				.process(hockeyGeoProcessor)
				.to("direct:backupAndElastic");

				

				from("direct:backupAndElastic")
					.multicast()
					.to("direct:elastic", "direct:backup");
				
				
				/*
				 * Ingest into elastic
				 */
				from("direct:elastic")
					.to(elastic)
						.to(result);
				
                JacksonDataFormat format = new JacksonDataFormat();

				from("direct:backup")
				.log("Before marshal: ${body}")
				.marshal(format)
				.log("After marshal: ${body}")
					.to(backupFileEndpoint);
			}
		};
	}
	
	@AfterClass
	public static void cleanup() throws IOException {
		// File outputDir = new File("src/test/resources/test-flow/baseball/.camel");
		List<String> dataDirs = new ArrayList<>();
		dataDirs.add(BASEBALLDIR);
		dataDirs.add(FOOTBALLDIR);
		dataDirs.add(HOCKEYDIR);
		dataDirs.add(BASKETBALLDIR);

		for (String dataDir : dataDirs) {
			File outputDir = new File(dataDir + "/.camel");
			if (outputDir.exists()) {
				for (File file : outputDir.listFiles()) {
					FileUtils.moveFileToDirectory(file, new File(dataDir), false);
				}
			}
			outputDir.delete();
		}
	}
}
