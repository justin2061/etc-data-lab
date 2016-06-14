package etc.dataprocess.router;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.ThreadPoolBuilder;
import org.apache.camel.builder.ThreadPoolProfileBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.component.jackson.ListJacksonDataFormat;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.spi.ThreadPoolProfile;
import org.apache.camel.spring.SpringRouteBuilder;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import static org.elasticsearch.common.xcontent.XContentBuilder.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import etc.dataprocess.es.CSVESTransportClient;
import etc.dataprocess.es.processor.M06ABaseIndexProcessor;

public class CSVImportRouter extends SpringRouteBuilder {

	Logger log = LoggerFactory.getLogger("csv-import-router");
	//private ExecutorService executorService;
	private int threadNum = 5;
	private String inputPath;
	String typeName = "m06a";
	String indexName;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	SimpleDateFormat idxSdf = new SimpleDateFormat("yyyyMMdd");

	@Resource(name = "es")
	private CSVESTransportClient esClient;
	
	@Resource(name = "m06aindex")
	private M06ABaseIndexProcessor m06aIndexProcessor;
	
	@PostConstruct
	public void init(){}
	
	@PreDestroy
	public void destroy(){}
	
	@Override
	public void configure() throws Exception {
		CamelContext context = this.getContext();
		
		ThreadPoolProfile customThreadPoolProfile =
	            new ThreadPoolProfileBuilder("customThreadPoolProfile")
					.poolSize(5)
					.maxPoolSize(5)
					.maxQueueSize(5)
					.build();
        context.getExecutorServiceManager().registerThreadPoolProfile(customThreadPoolProfile);
		
        onException(Exception.class)
		//.backOffMultiplier(1.2)
		.redeliveryDelay(30000)
		;
        
		/**
		 * CSV import
		 */
		//-- mass csv file
		from("file://"+inputPath+"?sortBy=file:modified&move=.done")
		.routeId("queue_result_s")
		.setHeader("esIndexName").simple("${file:onlyname.noext}")
		//.log("${header[esIndexName]}")
		.process(new Processor(){

			public void process(Exchange exchange) throws Exception {
				Map headers = exchange.getIn().getHeaders();
				Object body = exchange.getIn().getBody();
				indexName = (String) headers.get("esIndexName");
				indexName = indexName.toLowerCase();
				indexName = formatIndexName(indexName);
				esClient.createIndex(indexName, typeName);
				headers.put("esIndexName", indexName);
				exchange.getOut().setHeaders(headers);
				exchange.getOut().setBody(body);
				System.gc();
				headers = null;
			}})
		.unmarshal().csv()
		.convertBodyTo(List.class)
		.split(body()).streaming()
		//.threads(1)
		//.parallelProcessing()
		//.log("${header[esIndexName]} => ${body}")
		.to("direct:save_to_es")
		;
		
		from("direct:save_to_es")
		.routeId("save_to_es")
		.process(m06aIndexProcessor)
		.end()
		//.log("import ${header[esIndexName]} finish.")
		;
		
	}
	
	private String formatIndexName(String indexName){
		//-- input:TDCS_M06A_20140209_200000
		String indexDateName = StringUtils.split(indexName, "_")[2];//20140209
		String indexType = StringUtils.split(indexName, "_")[1];//M06A
		indexDateName = indexDateName.substring(4, 6);//02
		return indexType+"_"+indexDateName;//M06A_02
	}
	
	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public int getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}

	public CSVESTransportClient getEsClient() {
		return esClient;
	}

	public void setEsClient(CSVESTransportClient esClient) {
		this.esClient = esClient;
	}

	public M06ABaseIndexProcessor getEsbulkIndexProcessor() {
		return m06aIndexProcessor;
	}

	public void setEsbulkIndexProcessor(M06ABaseIndexProcessor esbulkIndexProcessor) {
		this.m06aIndexProcessor = esbulkIndexProcessor;
	}

	
	
}
