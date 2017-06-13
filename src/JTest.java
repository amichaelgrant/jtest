import java.io.FileOutputStream;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.mysql.jdbc.Statement;

public class JTest {
	
	static final String DBURL 		= "jdbc:mysql://localhost/jtest?useSSL=false";
	static final String DBUSER  	= "root";
	static final String DBPASS  	= "admin";
	
	Connection connection 			= null;
	Statement stmt 					= null;
	ResultSet rSet 					= null;
	
	int FetchSize 					= 1000;
	static final String CsvFilePath = "/tmp/data.csv";
	
	/**
	 * JTest
	 * Constructor; Initializes a connection to the database*/
	JTest(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
			if(connection == null) 
				throw new Exception("Error connecting to database");
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			
		}
	}
	
	/**
	 * StreamDataToCSV
	 * Given we could be processing millions of record,
	 * It only makes sense to stream the data in batches so as not to exhaust memory
	 * We will fetch the data in pages and process until the last record
	 * */
	public void StreamDataToCSV(){
		/**
		 * Open output file stream */
		FileWriter ostream = null;
		try{
			ostream = new FileWriter(CsvFilePath);
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		/**
		 * Starting reading records from table*/
		try{
			String line  = null;
			String sqlString = "SELECT * FROM location WHERE timestamp > '2017-05-01 00:00:00' and timestamp < '2017-05-01 23:59:59'";
			PreparedStatement pstmt = connection.prepareStatement(sqlString);
			pstmt.setFetchSize(FetchSize);
			rSet = pstmt.executeQuery();
			
			String advertiser_id = null;
			double latitude  = 0;
			double longitude = 0;
			double horizontal_accuracy = 0;
			String timestamp = null;
			while(rSet.next()){
				advertiser_id = rSet.getString("advertiser_id");
				latitude      = rSet.getDouble("latitude");
				longitude     = rSet.getDouble("longitude");
				horizontal_accuracy = rSet.getDouble("horizontal_accuracy");
				timestamp     = rSet.getString("timestamp");
				
				line = String.format("%s, %.4f, %.4f, %.4f, %s\n", advertiser_id, latitude, longitude, horizontal_accuracy, timestamp);
				ostream.write(line);
				System.out.println("Record=> " + line);
			}
			ostream.flush();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		/**
		 * Closing the connection to file stream*/
		try{
			if(ostream != null)
				ostream.close();
			if(connection != null)
				connection.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		System.out.println("Java test app");
		JTest jtest = new JTest();
		jtest.StreamDataToCSV();
	}
}
