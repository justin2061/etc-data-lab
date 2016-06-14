package etc.dataprocess.es;

import javax.annotation.*;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ESClient {

	Node node;
	Client client;
	private String clusterName;
	
	@PostConstruct
	public void init(){
		if(this.clusterName == null){
			this.clusterName = "CAW_ES";
		}
		node = NodeBuilder.nodeBuilder()
				.clusterName(clusterName)
				.data(false)
				.client(true)
				.node();
		client = node.client();
	}
	
	@PreDestroy
	public void destroy(){
		client.close();
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	
}
