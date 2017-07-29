package org.datadidit.elasticsearch;

import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.elasticsearch5.ElasticsearchEndpoint;
import org.apache.camel.component.file.FileEndpoint;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.component.jackson.ListJacksonDataFormat;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

/*
 * Test to see if I can unmarshal and reingest data in backup 
 * so no need to go back to google
 */
public class ElasticSearchIngestBackupDataCamelIT extends CamelTestSupport{
	private static final String INDEX_NAME = "test";
	
	private static final String INDEX_TYPE = "baseball";
	
	private final String BACKUPLOCATION = "src/test/resources/test-flow/backup";
	
	@EndpointInject(uri = "file://"+BACKUPLOCATION)
	protected FileEndpoint backupEndpoint;
	
	@EndpointInject(uri = "elasticsearch5://elasticsearch?indexName="+INDEX_NAME+"&indexType="+INDEX_TYPE+"&operation=BULK_INDEX&ip=127.0.0.1&port=9300")
	protected ElasticsearchEndpoint elastic;
	
	@EndpointInject(uri = "mock:result")
	protected MockEndpoint result;
	
	@Test
	public void testBackupFlow() throws InterruptedException {
		result.expectedMinimumMessageCount(1);
		
		result.assertIsSatisfied();
	}
	
	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			public void configure() {
                JacksonDataFormat format = new ListJacksonDataFormat();

				from(backupEndpoint)
				.unmarshal(format)
					.to(elastic)
					.to(result);
			}
		};
	}
}
