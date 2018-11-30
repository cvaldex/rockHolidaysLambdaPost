package cl.cvaldex.rockholiday.lambda.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import java.util.Collection;
//import java.util.Iterator;

import javax.sql.DataSource;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import cl.cvaldex.rockholiday.lambda.vo.TweetVO;

public class InsertTweetDAO {
	private DataSource ds;
	private int INSERT_KEY_INDEX = 7; //en debug se descubre que la septima columna es el indice
	//static final Logger logger = LogManager.getLogger(InsertTweetsDAO.class);

	public InsertTweetDAO(DataSource ds){
		this.ds = ds;
	}

	public long insertTweets(TweetVO tweet) throws IOException{
		PreparedStatement insertTweetPS = null;
		Connection conn = null;
		long key = -1;
		
		try {
			conn = ds.getConnection();

			insertTweetPS = conn.prepareStatement(assembleQuery(), Statement.RETURN_GENERATED_KEYS);

			//setear parámetros para la Query
			insertTweetPS.setString(1, tweet.getText());
			insertTweetPS.setDate(2, java.sql.Date.valueOf(tweet.getDate()));
			insertTweetPS.setString(3, tweet.getAuthor());

			setImage(insertTweetPS, 4 , tweet.getImage1());
			setImage(insertTweetPS, 5 , tweet.getImage2());
			setImage(insertTweetPS, 6 , tweet.getImage3());
			setImage(insertTweetPS, 7 , tweet.getImage4());
	
			insertTweetPS.execute();
			
			key = getInsertedKey(insertTweetPS.getGeneratedKeys());
			
			//cerrar elementos de colección a BD
			conn.close();
			insertTweetPS.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return key;
	}
	
	private String assembleQuery(){
		StringBuilder builder = new StringBuilder();
		
		builder.append("INSERT INTO public.tweets (tweet , eventdate , author , image1 , image2 , image3 , image4) ");
		builder.append("VALUES (? , ? , ? , ? , ? , ? , ?) ");
		
		return builder.toString();
	}
	
	private void setImage(PreparedStatement pstmt , int index , InputStream input) throws SQLException, IOException{
		if(input != null){
			pstmt.setBinaryStream(index, input, input.available());
		}
		else{
			pstmt.setNull(index, java.sql.Types.BINARY);
		}
	}
	
	private long getInsertedKey(ResultSet keys) throws SQLException{
		long key = -1;
		
		if (keys.next()) {
			key = keys.getLong(INSERT_KEY_INDEX);
        }
        else {
            throw new SQLException("Creating tweet failed, no ID obtained.");
        }
		
		return key;
	}
}
