package cl.cvaldex.rockholiday.lambda;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import javax.sql.DataSource;

import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import cl.cvaldex.rockholiday.lambda.factory.LambdaLoggerFactory;
import cl.cvaldex.rockholiday.lambda.jdbc.InsertTweetDAO;
import cl.cvaldex.rockholiday.lambda.vo.TweetVO;

import org.json.simple.JSONObject;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.ds.common.BaseDataSource;
import org.json.simple.parser.JSONParser;

import org.apache.commons.codec.binary.Base64;

public class InsertTweetFromStream implements RequestStreamHandler {

	public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
		BaseDataSource dataSource = null;
		InsertTweetDAO dao = null;
		long id = -1000;
		
		LambdaLogger logger = LambdaLoggerFactory.getLogger(context);
		
		logger.log("Getting DataSource");
		dataSource = getDatasource();
		
		logger.log("Loading Java Lambda handler of InsertTweetFromStream");
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		
		JSONParser parser = new JSONParser();
		JSONObject responseJson = new JSONObject();

		String responseCode = "200";

		try {
			JSONObject parsedJSON = (JSONObject)parser.parse(reader);

			TweetVO vo = new TweetVO();
			
			vo.setText(evaluateAndAsign("text" , parsedJSON.get("text") , false));
			vo.setDate(evaluateAndAsign("date" , parsedJSON.get("date") , false));
			vo.setAuthor(evaluateAndAsign("author" , parsedJSON.get("author") , false));
			vo.setImage1(decodeBase64(evaluateAndAsign("image1" , parsedJSON.get("image1"), true)));
			vo.setImage2(decodeBase64(evaluateAndAsign("image2" , parsedJSON.get("image2"), true)));
			vo.setImage3(decodeBase64(evaluateAndAsign("image3" , parsedJSON.get("image3"), true)));
			vo.setImage4(decodeBase64(evaluateAndAsign("image4" , parsedJSON.get("image4"), true)));
			
			logger.log(vo.toString());
			
			//Insertar en la Base de Datos
			dao = new InsertTweetDAO((DataSource) dataSource);
			
			id = dao.insertTweets(vo);
			
			responseJson.put("id", id);  
		} catch(Exception pex) {
			responseCode = "400";
			responseJson.put("exception", pex);
		}
		
		responseJson.put("statusCode", responseCode);

		logger.log(responseJson.toJSONString());
		
		OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");
		
		writer.write(responseJson.toJSONString());  
		writer.close();
	}
	
	public String evaluateAndAsign(String fieldName , Object value , boolean useEmptyString) throws Exception{
		if(value == null && !useEmptyString){
			throw new Exception (fieldName + ": cannot be null");
		}
		
		String stringValue = (value == null) ? "" : (String)value;
		
		return stringValue;
	}
	

	private InputStream decodeBase64(String encodedFile){
		InputStream byteFile = null;
		
		if(encodedFile != null){
			if(encodedFile.trim().length() > 0){
				byteFile = new ByteArrayInputStream(Base64.decodeBase64(encodedFile.getBytes()));
			}
		}

		return byteFile;
	}
	
	private BaseDataSource getDatasource(){
		BaseDataSource dataSource = new PGSimpleDataSource();
		
		dataSource.setServerName(getProperty("dbServerName"));
		dataSource.setPortNumber(new Integer(getProperty("dbServerPort")));
		dataSource.setDatabaseName(getProperty("dbName"));
		dataSource.setUser(getProperty("dbUserName"));
		dataSource.setPassword(getProperty("dbPassword"));
		
		return dataSource;
	}
	
	private String getProperty(String property){
		String value = System.getenv(property);
		
		if(value == null){
			value = System.getProperty(property);
		}
		
		return value;
	}
}