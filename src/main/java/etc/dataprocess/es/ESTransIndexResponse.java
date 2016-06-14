package etc.dataprocess.es;

public class ESTransIndexResponse {

	private String indexName;
	private String typeName;
	private String id;
	private Long version;
	private boolean created;
	
	public ESTransIndexResponse(){}
	
	public String getIndexName() {
		return indexName;
	}
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	public String getTypeName() {
		return typeName;
	}
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	public Long getVersion() {
		return version;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	public boolean isCreated() {
		return created;
	}

	public void setCreated(boolean created) {
		this.created = created;
	}

	public String toString(){
		return org.apache.commons.lang.builder.ToStringBuilder.reflectionToString(this).toString();
	}
	
}
