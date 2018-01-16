package general;

public class Executor {
	String host;
	Long port;
	String index;
	String id;
//	String tid;

//	public Executor(String host, Long port, String tid){
//		this.host = host;
//		this.port = port;
//		this.tid = tid;
//	}

	public Executor(String host, Long port){
		this.host = host;
		this.port = port;
		this.index = host+"_"+ port;
		this.id = "";
	}

	public Executor(String host, Long port, String id){
		this.host = host;
		this.port = port;
		this.index = host+"_"+ port;
		this.id = id;
	}
	
	public String getIndex() {
		return index;
	}


	public void setIndex(String index) {
		this.index = index;
	}


	@Override
	public String toString() {
		return "[ "+host + ", " + port + "]";
	}
	
	
//	public String getTid() {
//		return tid;
//	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Long getPort() {
		return port;
	}

	public void setPort(Long port) {
		this.port = port;
	}


	public String getId() {
		return id;
	}


	public void setId(String id) {
		this.id = id;
	}




//	public void setTid(String tid) {
//		this.tid = tid;
//	}
	
}
