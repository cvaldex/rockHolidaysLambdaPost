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

import cl.cvaldex.rockholiday.lambda.factory.DataSourceFactory;
import cl.cvaldex.rockholiday.lambda.factory.LambdaLoggerFactory;
import cl.cvaldex.rockholiday.lambda.jdbc.InsertTweetDAO;
import cl.cvaldex.rockholiday.lambda.vo.TweetVO;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.postgresql.ds.common.BaseDataSource;
import org.json.simple.parser.JSONParser;

import org.apache.commons.codec.binary.Base64;

public class InsertTweetFromStream implements RequestStreamHandler {
	
	/*
	 * Este campo permite determinar si la clase debe sacar los datos desde el factory local
	 * o desde el entorno de ejecución
	 * 
	 * Por defecto, los saca del entorno para respetar el stateless del Lambda
	 */
	private boolean isLocal = false;

	public InsertTweetFromStream(boolean isLocal){
		this.isLocal = isLocal;
	}
	
	public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
		BaseDataSource dataSource = null;
		InsertTweetDAO dao = null;
		long id = -1000;
		
		LambdaLogger logger = LambdaLoggerFactory.getLogger(context);
		logger.log("Getting DataSource");
		dataSource = (BaseDataSource)DataSourceFactory.getDataSource(isLocal);
		
		logger.log("Loading Java Lambda handler of InsertTweetFromStream");
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		
		JSONParser parser = new JSONParser();
		JSONObject responseJson = new JSONObject();

		String responseCode = "200";

		try {
			JSONObject event = (JSONObject)parser.parse(reader);

			if (event.get("body") != null) {
				JSONObject body = (JSONObject)event.get("body");

				TweetVO vo = new TweetVO();

				if ( body.get("text") != null) {
					vo.setText((String)body.get("text"));
				}

				if ( body.get("date") != null) {
					vo.setDate((String)body.get("date"));
				}

				if ( body.get("author") != null) {
					vo.setAuthor((String)body.get("author"));
				}
				
				if ( body.get("image1") != null) {
					vo.setImage1(decodeBase64((String)body.get("image1")));
				}
				
				if ( body.get("image2") != null) {
					vo.setImage2(decodeBase64((String)body.get("image2")));
				}
				
				if ( body.get("image3") != null) {
					vo.setImage3(decodeBase64((String)body.get("image3")));
				}
				
				if ( body.get("image4") != null) {
					vo.setImage4(decodeBase64((String)body.get("image4")));
				}
				
				logger.log(vo.toString());
				
				//Insertar en la Base de Datos
				dao = new InsertTweetDAO((DataSource) dataSource);
				
				id = dao.insertTweets(vo);
			}

			JSONObject headerJson = new JSONObject();
			//headerJson.put("x-custom-header", "my custom header value");

			responseJson.put("isBase64Encoded", false);
			responseJson.put("statusCode", responseCode);
			responseJson.put("headers", headerJson);
			responseJson.put("id", id);  

		} catch(ParseException pex) {
			responseJson.put("statusCode", "400");
			responseJson.put("exception", pex);
		}

		logger.log(responseJson.toJSONString());
		
		OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");
		
		writer.write(responseJson.toJSONString());  
		writer.close();
	}

	private InputStream decodeBase64(String encodedFile){
		InputStream byteFile = new ByteArrayInputStream(Base64.decodeBase64(encodedFile.getBytes()));

		return byteFile;
	}
}