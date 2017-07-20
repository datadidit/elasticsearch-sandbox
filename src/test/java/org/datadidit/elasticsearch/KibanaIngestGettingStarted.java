package org.datadidit.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Windows doesn't have necessary curl commands and all the other solutions seem hacking so 
 * just gonna ingest w/ an HTTP Client
 *
 */
public class KibanaIngestGettingStarted {	
	private HttpClient client = HttpClientBuilder.create().build();

	private static Map<String, String> headers = new HashMap<>();
	
	@BeforeClass
	public static void setup(){
		headers.put("Content-Type", "application/json");	
	}
	
	@Test
	public void ingestLogs() throws IOException{
		File logStashData = new File("src/test/resources/data/logs.jsonl");
		
		List<String> lines = FileUtils.readLines(logStashData);
		
		StringBuilder build = new StringBuilder();
		for(String json : lines){
			build.append(json+"\n");
		}
		Integer code = this.sendPost("http://localhost:9200/_bulk?pretty", headers, build.toString());
		System.out.println("Code: "+code);
	}
	
	@Test
	public void ingestShakespeare() throws ClientProtocolException, IOException{
		File shakespeareData = new File("src/test/resources/data/shakespeare.json");
		
		Integer code = this.sendPost("http://localhost:9200/shakespeare/_bulk?pretty", headers, 
				FileUtils.readFileToString(shakespeareData));
		
		System.out.println(code);
	}
	
	private Integer sendPost(String url, Map<String, String> headers, String data) throws ClientProtocolException, IOException{
		HttpPost post = new HttpPost(url);
		
		for(Map.Entry<String, String> entry : headers.entrySet()){
			post.setHeader(entry.getKey(), entry.getValue());
		}
		
        HttpEntity entity = new ByteArrayEntity(data.getBytes("UTF-8"));
		post.setEntity(entity);
		HttpResponse response = client.execute(post);
		
		return response.getStatusLine().getStatusCode();
	}
}
