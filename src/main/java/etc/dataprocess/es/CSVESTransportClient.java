package etc.dataprocess.es;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public class CSVESTransportClient {

	private Client client;
	private String EShost;
	private Logger log = LoggerFactory.getLogger("es-client");
	private String clusterName;
	private Long indexTimeout;// default 5 sec.
	private int esPort;
	private BulkProcessor esbulk;
	int bulkActions = 1000;
	long bulkMBSize = 10;
	long bulkFlashSec = 1;
	
	@PostConstruct
	public void init(){
		if(EShost == null){
			EShost = "10.64.32.46";
		}
		try {
			Settings settings = Settings.settingsBuilder()
			        .put("cluster.name", clusterName)
			        .build();
			client = TransportClient.builder()
					.settings(settings)
					.build()
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(EShost), esPort))
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.64.32.49"), esPort))
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.64.32.50"), esPort))
			        ;
			//log.info(client.settings().toString());
		} catch (UnknownHostException e) {
			log.error(e.getLocalizedMessage());
		}
		
		esbulk = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			
			public void beforeBulk(long arg0, BulkRequest req) {
				//log.info("bulk request: "+req.toString());
				
			}
			
			public void afterBulk(long arg0, BulkRequest req, Throwable fail) {
				//log.error(req.toString() + " -> "+ fail.getLocalizedMessage());
				log.error(fail.getLocalizedMessage(), fail);
				//fail.printStackTrace();
			}
			
			public void afterBulk(long arg0, BulkRequest bulkReq, BulkResponse bulkRes) {
				//log.info("get took in millis: "+ bulkRes.getTookInMillis());
				//log.info("is fail ? ["+bulkRes.hasFailures()+"]");
			}
		})
		.setBulkSize(new ByteSizeValue(bulkMBSize, ByteSizeUnit.MB))
		.setBulkActions(bulkActions)
		.setFlushInterval(TimeValue.timeValueSeconds(bulkFlashSec))
		//.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(200), 5)) 
        .setConcurrentRequests(10)
        .build();
		;
	}
	
	@PreDestroy
	public void destroy(){
		/*
		try {
			esbulk.awaitClose(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			log.error(e.getLocalizedMessage());
		}
		*/
		esbulk.flush();
		esbulk.close();
		client.close();
	}

	public void createIndex(String indexName, String typeName){
		final IndicesExistsResponse res = client.admin()
						.indices().prepareExists(indexName)
						.execute().actionGet()
						;
		if(!res.isExists()){
			
			//CreateIndexResponse createIdxRes = 
					client.admin().indices()
						.prepareCreate(indexName)
						.setSettings(Settings.builder()
										.put("index.number_of_shards", 5)
										.put("index.number_of_replicas", 1)
									)
						.get();
			
			
		}else{}
			
		GetSettingsResponse response = client.admin().indices()
		        .prepareGetSettings(indexName).get();                           
		for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) { 
		    String index = cursor.key;                                                      
		    Settings settings = cursor.value;                                               
		    Integer shards = settings.getAsInt("index.number_of_shards", null);             
		    Integer replicas = settings.getAsInt("index.number_of_replicas", null);         
		}
	}
	
	public ESTransIndexResponse index(String indexName, String typeName, Map data){
		
		IndexResponse response = client
									.prepareIndex(indexName, typeName)
									.setSource(data)
									.setTimeout(TimeValue.timeValueSeconds(indexTimeout))
									.get(TimeValue.timeValueSeconds(indexTimeout));
		ESTransIndexResponse indexResponse = new ESTransIndexResponse();
		indexResponse.setId(response.getId());
		indexResponse.setIndexName(response.getIndex());
		indexResponse.setTypeName(response.getType());
		indexResponse.setVersion(response.getVersion());
		indexResponse.setCreated(response.isCreated());
		return indexResponse;
	}
	
	public void bulkIndex(String indexName, String typeName, Map<String, Object> data){
		IndexRequest indexRequest;
		try {
			indexRequest = client.prepareIndex(indexName, typeName).setSource(data).request();
			esbulk.add(indexRequest);
		} catch (Exception e) {
			log.info(e.getLocalizedMessage());
		}finally{
			//esbulk.flush();
		}
	}
	
	public void bulkIndex(String indexName, String typeName, XContentBuilder data){
		IndexRequest indexRequest;
		try {
			indexRequest = client.prepareIndex(indexName, typeName).setSource(data).request();
			esbulk.add(indexRequest);
		} catch (Exception e) {
			log.info(e.getLocalizedMessage());
		}finally{
			//esbulk.flush();
		}
	}
	
	public void closeBulkIndex(){
		esbulk.close();
	}
	
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public Long getIndexTimeout() {
		return indexTimeout;
	}

	public void setIndexTimeout(Long indexTimeout) {
		this.indexTimeout = indexTimeout;
	}

	public String getEShost() {
		return EShost;
	}

	public void setEShost(String eShost) {
		EShost = eShost;
	}

	public int getEsPort() {
		return esPort;
	}

	public void setEsPort(int esPort) {
		this.esPort = esPort;
	}

	public int getBulkActions() {
		return bulkActions;
	}

	public void setBulkActions(int bulkActions) {
		this.bulkActions = bulkActions;
	}

	public long getBulkMBSize() {
		return bulkMBSize;
	}

	public void setBulkMBSize(long bulkMBSize) {
		this.bulkMBSize = bulkMBSize;
	}

	public long getBulkFlashSec() {
		return bulkFlashSec;
	}

	public void setBulkFlashSec(long bulkFlashSec) {
		this.bulkFlashSec = bulkFlashSec;
	}
	
}
