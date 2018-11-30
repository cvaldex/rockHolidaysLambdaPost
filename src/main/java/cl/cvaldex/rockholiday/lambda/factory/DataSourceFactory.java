package cl.cvaldex.rockholiday.lambda.factory;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.ds.common.BaseDataSource;

public class DataSourceFactory {
	public static DataSource getDataSource(boolean isLocal){
		BaseDataSource ds = new PGSimpleDataSource();
		Map<String, String> properties = null;
		
		if(isLocal){
			properties = getLocalProperties();
		}
		
		ds.setServerName(properties.get("serverName"));
		ds.setPortNumber(new Integer(properties.get("serverPort")).intValue());
		ds.setDatabaseName(properties.get("dbName"));
		ds.setUser(properties.get("dbUserName"));
		ds.setPassword(properties.get("dbPassword"));
		
		return (DataSource)ds;
	}
	
	public static Map<String, String> getLocalProperties(){
		Map<String, String> properties = new HashMap<String, String>();
		
		properties.put("serverName", "rockholidays.cvecralyfpim.us-east-1.rds.amazonaws.com");
		properties.put("serverPort", "5432");
		properties.put("dbName", "rockholidays");
		properties.put("dbUserName", "rockholidays");
		properties.put("dbPassword", "rockholidays2018");
		
		return properties;
	}
}
