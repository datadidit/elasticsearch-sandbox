package org.datadidit.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.direct.DirectEndpoint;
import org.apache.camel.component.elasticsearch5.ElasticsearchEndpoint;
import org.apache.camel.component.file.FileEndpoint;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.component.jackson.ListJacksonDataFormat;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultProducerTemplate;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;

import datadidit.helpful.hints.camel.CSVToJsonProcessor;

public class ElasticSearchCamelDateIT extends CamelTestSupport {
	private static final String INDEX_NAME = "test";

	private static final String INDEX_TYPE = "baseball";

	private static final String HOSTNAME = "127.0.0.1";

	private static final String BASEBALLDIR = "src/test/resources/test-flow/baseball";

	@EndpointInject(uri = "file://" + BASEBALLDIR)
	protected FileEndpoint baseballFileEndpoint;

	@EndpointInject(uri = "direct:elastic")
	protected DirectEndpoint directElastic;

	@EndpointInject(uri = "elasticsearch5://elasticsearch?indexName=" + INDEX_NAME + "&indexType=" + INDEX_TYPE
			+ "&operation=BULK_INDEX&ip=127.0.0.1&port=9300")
	protected ElasticsearchEndpoint elastic;

	@EndpointInject(uri = "mock:result")
	protected MockEndpoint result;
	
	@Test
	public void testFlow() throws InterruptedException {
		Integer expectedCount = 1;
		Integer waitTime = 60;
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
				try {

					CSVToJsonProcessor csvProcessor = new CSVToJsonProcessor(true, null);
	                JacksonDataFormat format = new ListJacksonDataFormat();

					// Need to set default Endpoint
					DefaultProducerTemplate template = new DefaultProducerTemplate(this.getContext(), directElastic);
					template.start();
					csvProcessor.setProducer(template);

					List<Map<String, Object>> obj = new ArrayList<>();
					
					from(baseballFileEndpoint)
						.process(csvProcessor);

					from(directElastic)
						.unmarshal(format)
							.log("Message ${body}")
								.to(elastic)
									.to(result);
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
	}
	
	@AfterClass
	public static void cleanup() throws IOException {
		// File outputDir = new File("src/test/resources/test-flow/baseball/.camel");
		List<String> dataDirs = new ArrayList<>();
		dataDirs.add(BASEBALLDIR);
		
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
