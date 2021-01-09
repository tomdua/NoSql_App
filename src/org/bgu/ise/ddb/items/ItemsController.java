/**
 * 
 */
package org.bgu.ise.ddb.items;

import com.mongodb.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.IOException;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {

	/**
	 * The function copy all the items(title and production year) from the Oracle
	 * table MediaItems to the System storage. The Oracle table and data should be
	 * used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method = { RequestMethod.GET })
	public void fillMediaItems(HttpServletResponse response){
		System.out.println("was here");
		ArrayList<MediaItems> mediaItems = new ArrayList<MediaItems>();
		PreparedStatement ps = null;
		Connection connectionOracle = null;
		ResultSet rs=null;
		BasicDBObject dbObject;
		try 
		{
			connectionOracle = ConnectToOracle();
			connectionOracle.setAutoCommit(false);
			String query = "SELECT title,prod_year FROM MediaItems";
			ps = connectionOracle.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				String title =rs.getString(1);
				int year = rs.getInt(2);
				MediaItems itemToAdd = new MediaItems(title, year);
				mediaItems.add(itemToAdd);
			}
			rs.close();
			for (MediaItems mediaItem : mediaItems) {
				MongoClient mongoClient = ConnectToDB();
				DBCollection dbCollection = mongoClient.getDB("project3DB").getCollection("MediaItems");
				dbObject = new BasicDBObject();
				dbObject.put("Title", mediaItem.getTitle());
				DBCursor cursor = dbCollection.find(dbObject);
				boolean isExist = checkIfExists(cursor);
				if(!isExist){
					BasicDBObject item = new BasicDBObject();
					item.put("Title", mediaItem.getTitle());
					item.put("Year", mediaItem.getProdYear());
					dbCollection.insert(item);
				}
				mongoClient.close();
			}
			
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if (connectionOracle != null) {
					connectionOracle.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	

		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	

	/**
	 * The function copy all the items from the remote file, the remote file have
	 * the same structure as the films file from the previous assignment. You can
	 * assume that the address protocol is http
	 * 
	 * @throws IOException
	 */
	@RequestMapping(value = "fill_media_items_from_url", method = { RequestMethod.GET })
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);		
		URL url = new URL(urladdress);
		BufferedReader br = null;
		String line = "";
		InputStreamReader streamReader=null;
		BasicDBObject dbObject=null;
		try {
			streamReader = new InputStreamReader(url.openStream());
			br = new BufferedReader(new BufferedReader(streamReader));
			while ((line = br.readLine()) != null) {
				String[] splitLine = line.split(",");
				String title= splitLine[0];
				String yearString=splitLine[1];
				int year = Integer.parseInt(yearString);
				MongoClient mongoClient =ConnectToDB();
				DBCollection dbCollection = mongoClient.getDB("project3DB").getCollection("MediaItems");
				dbObject = new BasicDBObject();
				dbObject.put("Title", title);
				DBCursor cursor = dbCollection.find(dbObject);
				boolean isExist = checkIfExists(cursor);
				if(!isExist){
					BasicDBObject item = new BasicDBObject();
					item.put("Title", title);
					item.put("Year", year);
					dbCollection.insert(item);
				}
				mongoClient.close();
			}
		} catch (Exception e) {
			System.out.println(e);
		}	
		
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	/**
	 * The function retrieves from the system storage N items, order is not
	 * important( any N items)
	 * 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){		
		ArrayList<MediaItems> topNItems = new ArrayList<MediaItems>();
		try {
			MongoClient mongoClient = ConnectToDB();
			DBCollection dbCollection = mongoClient.getDB("project3DB").getCollection("MediaItems");
			DBCursor cursor = dbCollection.find().limit(topN);
			while (cursor.hasNext()) 
			{ 
				DBObject current = cursor.next();
				String title = (String)current.get("Title");
				int year = (int)current.get("Year");
				MediaItems item = new MediaItems(title, year);
				topNItems.add(item);
			}
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e.getStackTrace());
		}
		int arraySize=topNItems.size();
		MediaItems[] itemToAdd = new MediaItems[arraySize];
		return topNItems.toArray(itemToAdd);
		
	}

	private static Connection ConnectToOracle() {
		// TODO Auto-generated method stub
		Connection connection = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String connectionUrl = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/oracle";
			connection = DriverManager.getConnection(connectionUrl, "almogsa", "abcd");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;
	}

	private static MongoClient ConnectToDB() {
		Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);
		MongoClientURI uri = new MongoClientURI(
				"mongodb+srv://ass3:asd123456@ass3cluster.qqs8l.mongodb.net/test?retryWrites=true&w=majority");

		MongoClient mongoClient = new MongoClient(uri);


		return mongoClient;
	}


	private static boolean checkIfExists(DBCursor dbCursor) {
		boolean result=false;
		while (dbCursor.hasNext()) 
		{ 
			result = true;
			dbCursor.next();
		}
		return result;
	}

}
