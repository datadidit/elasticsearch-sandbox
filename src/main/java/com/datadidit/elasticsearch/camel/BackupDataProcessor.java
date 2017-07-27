package com.datadidit.elasticsearch.camel;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BackupDataProcessor implements Processor{
	private ObjectMapper mapper = new ObjectMapper();


	@Override
	public void process(Exchange exchange) throws Exception {
		InputStream stream = exchange.getIn().getBody(InputStream.class);
		
		//Convert Input into Data Structure representing Json
		Object jsonDS = this.stringToJson(stream);
		
		List<Map<String, Object>> output = new ArrayList<>();
		
		if(jsonDS instanceof List){
			output = (List<Map<String, Object>>) jsonDS;
		}else{
			Map<String, Object> temp = (Map<String, Object>) jsonDS;
			output.add(temp);
		}
		
		exchange.getIn().setBody(output);
	}
	
	public Object stringToJson(InputStream stream) throws IOException{        
        Map<String,Object> map = new HashMap<>();
		
        String input = IOUtils.toString(stream, Charset.defaultCharset());
        
        /*
         * Dealing with Map
         */
        if(input.startsWith("[")){
        	List<Map<String, Object>> convertList = new ArrayList<>();
        	
        	return mapper.readValue(input, convertList.getClass());
        }
        
        return mapper.readValue(input, map.getClass());
	}
}
