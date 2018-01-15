package name.nmarchenko.org.apache.storm.statefulbolwarmcache;

public class ConfigurationException extends Exception {
	private static final long serialVersionUID = 1L;

	public ConfigurationException() {}
	
	public ConfigurationException(String message){
		super(message);
	}
}
