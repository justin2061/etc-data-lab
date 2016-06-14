package etc.dataprocess.es;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESTransportClient {

	private Client client;
	private String EShost;
	private Logger log = LoggerFactory.getLogger("es-client");
	private String clusterName;
	private Long indexTimeout;// default 5 sec.
	private int esPort;
	private BulkProcessor esbulk;
	int bulkActions = 500;
	long bulkMBSize = 1;
	long bulkFlashSec = 5;
	
	@PostConstruct
	public void init(){
		if(EShost == null){
			EShost = "10.96.21.17";
		}
		try {
			Settings settings = Settings.settingsBuilder()
			        .put("cluster.name", clusterName)
			        .build();
			client = TransportClient.builder()
					.settings(settings)
					.build()
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(EShost), esPort))
			        ;
		} catch (UnknownHostException e) {
			log.error(e.getLocalizedMessage());
		}
		
		esbulk = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			
			public void beforeBulk(long arg0, BulkRequest arg1) {
				// TODO Auto-generated method stub
				
			}
			
			public void afterBulk(long arg0, BulkRequest arg1, Throwable arg2) {
				// TODO Auto-generated method stub
				
			}
			
			public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {
				// TODO Auto-generated method stub
				
			}
		})
		.setBulkSize(new ByteSizeValue(bulkMBSize, ByteSizeUnit.MB))
		.setBulkActions(bulkActions)
		.setFlushInterval(TimeValue.timeValueSeconds(bulkFlashSec))
		.setBackoffPolicy(
            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(200), 5)) 
        .build();
		;
	}
	
	@PreDestroy
	public void destroy(){
		client.close();
	}

	public void createIndex(String indexName, String typeName){
		final IndicesExistsResponse res = client.admin()
						.indices().prepareExists(indexName)
						.execute().actionGet()
						;
		/*
		if(res.isExists()){
			final DeleteIndexRequestBuilder delIdx = client.admin().indices().prepareDelete(indexName);
			delIdx.execute().actionGet();
		}
		*/
		/*
		try {
			final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
					.startObject()
						.startObject(typeName)
							.field("host","string")
							.field("index","not_analyzed")
						.endObject()
					.endObject()
					;
			client.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(mappingBuilder).execute().actionGet();
		} catch (IOException e) {
			log.error(e.getLocalizedMessage());
		}
		*/
	}
	
	public ESTransIndexResponse index(String indexName, String typeName, Map data){
		/*
		CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(indexName);
		CreateIndexResponse cir = null;
		try {
			final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
					.startObject()
						.startObject(typeName)
							.field("host","string")
							.field("index","not_analyzed")
						.endObject()
					.endObject()
					;
			log.info(mappingBuilder.string());
			cir = cirb.addMapping(typeName, mappingBuilder)
			.setSource(data)
			.setTimeout(TimeValue.timeValueSeconds(indexTimeout))
			.execute().actionGet()
			;
			
		} catch (IOException e) {
			log.error(e.getLocalizedMessage());
		}
		*/
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
	
	public void bulkIndex(String indexName, String typeName, Map data){
		
		try {
			esbulk.add(new IndexRequest(indexName, typeName).source(data));
			esbulk.awaitClose(10, TimeUnit.MINUTES);
		} catch (Exception e) {
			log.info(e.getLocalizedMessage());
		}
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
	
}
