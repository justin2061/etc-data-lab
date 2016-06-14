package etc.dataprocess.es.processor;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import etc.dataprocess.es.CSVESTransportClient;

public class M06ABaseIndexProcessor implements Processor {

	Logger log = LoggerFactory.getLogger("es-bulk-index");
	
	@Resource(name = "es")
	private CSVESTransportClient esClient;
	
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	String indexName;
	String typeName = "m06a";
	
	public void createIndex(String indexName){
		indexName = indexName.toLowerCase();
		indexName = formatIndexName(indexName);
		esClient.createIndex(indexName, typeName);
	}
	
	private String formatIndexName(String indexName){
		//-- input:TDCS_M06A_20140209_200000
		String indexDateName = StringUtils.split(indexName, "_")[2];//20140209
		String indexType = StringUtils.split(indexName, "_")[1];//M06A
		indexDateName = indexDateName.substring(4, 6);//02
		return indexType+"_"+indexDateName;//M06A_02
	}
	
	public void process(Exchange exchange) throws Exception {
		//log.info(exchange.getIn().getClass().getName());
		List<String> line = (List<String>)exchange.getIn().getBody();
		Map headers = exchange.getIn().getHeaders();
		Map<String, Object> data = new HashMap<String, Object>();
		indexName = (String) headers.get("esIndexName");
		indexName = indexName.toLowerCase();
		
		_bulkIndex(line, data);
		
		exchange.getOut().setHeaders(headers);
		exchange.getOut().setBody(data);
		headers = null;
	}

	private void _bulkIndex(List<String> line, Map<String, Object> data) {
		Date date = Calendar.getInstance().getTime();
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder()
							.startObject()
								.field("VehicleType", (String)line.get(0))
								.field("DetectionTime_O", formatESDate(line.get(1)))
								.field("GantryID_O", (String)line.get(2))
								.field("DetectionTime_D", formatESDate(line.get(3)))
								.field("GantryID_D", (String)line.get(4))
								.field("TripLength", Float.parseFloat(line.get(5)))
								.field("TripEnd", (String)line.get(6))
								.field("TripInformation", (String)line.get(7))
								.field("updated", date)
							.endObject();
			esClient.bulkIndex(indexName, typeName, builder);
		} catch (Exception e) {
			log.error(line+", err: "+e.getMessage());
		}
	}
	
	/*
	private void _bulkIndex_(List<String> line, Map<String, Object> data) {
		try{
			data.put("VehicleType", (String)line.get(0));
			data.put("DetectionTime_O", formatESDate(line.get(1)) );
			data.put("GantryID_O", (String)line.get(2));
			data.put("DetectionTime_D", formatESDate(line.get(3)) );
			data.put("GantryID_D", (String)line.get(4) );
			data.put("TripLength", Float.parseFloat(line.get(5)) );
			data.put("TripEnd", (String)line.get(6) );
			data.put("TripInformation", (String)line.get(7) );
			//log.info(data.toString());
			//ESTransIndexResponse response = esClient.index(indexName, typeName, data);
			//log.info(response.toString());
			esClient.bulkIndex(indexName, typeName, data);
		}catch(Exception e){
			log.error("index:["+indexName+"], line["+line.toString()+"] has error: "+e.getLocalizedMessage());
		}
	}
	*/
	
	private synchronized java.util.Date formatESDate(String dateString) throws Exception{
		if(dateString.trim().length() == 0){
			throw new Exception("input date string is empty");
		}
		
		Date dd = new Date();
		try {
			dd = sdf.parse(dateString);
		} catch (Exception e) {
			String err = dateString+", err: "+e.getLocalizedMessage();
			log.error(err);
			throw new Exception(err);
		}
		return dd;
		//return sdf.parse(dateString);
	}
}
