/**
 * 
 */
package org.bgu.ise.ddb.history;
import java.util.ArrayList;
import com.mongodb.*;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import java.util.HashSet;
import java.util.Set;
/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{



	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		System.out.println(username+" "+title);
		BasicDBObject query = new BasicDBObject();
		DBCursor dBcursor = null;
		MongoClient mongoClient;
		DBCollection historyTable,mediaItemsTable,usersTable;
		boolean succses=false;
		query = new BasicDBObject();
		try{
			mongoClient = ConnectToDB();
			historyTable = mongoClient.getDB("project3DB").getCollection("UsersHistory");
			boolean itemExits = false;
			mediaItemsTable = mongoClient.getDB("project3DB").getCollection("MediaItems");
			query.put("Title", title);
			dBcursor = mediaItemsTable.find(query);
			itemExits = checkIfExits(dBcursor);

			usersTable = mongoClient.getDB("project3DB").getCollection("Users");
			query = new BasicDBObject();
			query.put("UserName", username);
			dBcursor = usersTable.find(query);
			boolean userExist = false;
			userExist = checkIfExits(dBcursor);

			if(userExist && itemExits){
				query = new BasicDBObject();
				query.put("UserName", username);
				query.put("Title", title);
				query.put("Timestamp", new Date().getTime());
				historyTable.insert(query);
			}
			mongoClient.close();
			succses=true;
		} catch (Exception e) {
			System.out.println(e);

		}
		if (succses)
		{		
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
		}
	}



	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		MongoClient mongoClient;
		DBCollection  userHistryTable;
		BasicDBObject query;
		DBCursor DBcursor;
		BasicDBObject TimestampFromDB;
		HistoryPair historyp;
		ArrayList<HistoryPair> pairs = new ArrayList<>();
		try{
			mongoClient = ConnectToDB();
			userHistryTable = mongoClient.getDB("project3DB").getCollection("UsersHistory");
			query = new BasicDBObject();
			query.put("UserName", username);
			TimestampFromDB = new BasicDBObject("Timestamp",-1);
			DBcursor = userHistryTable.find(query).sort(TimestampFromDB);
			while (DBcursor.hasNext()) 
			{ 
				DBObject curHistoryPair = DBcursor.next();
				String title = (String)curHistoryPair.get("Title");
				long timestamp = (long)curHistoryPair.get("Timestamp");
				Date new_date = new Date(timestamp);
				historyp = new HistoryPair(title,new_date);
				pairs.add(historyp);
			}	
			mongoClient.close();
		}catch(Exception e){
			System.out.println(e);
		}
		int arraySize=pairs.size();
		HistoryPair[] historypArray = new HistoryPair[arraySize];
		return pairs.toArray(historypArray);

	}

	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		ArrayList<HistoryPair> pairs = new ArrayList<>();
		DBCursor DBcursor;
		BasicDBObject query;
		DBCollection  userHistoryTable;
		DBObject currHistoryPair;
		HistoryPair historyp;
		try{
			MongoClient mongoClient = ConnectToDB();
			userHistoryTable = mongoClient.getDB("project3DB").getCollection("UsersHistory");
			query = new BasicDBObject();
			query.put("Title", title);
			DBcursor = userHistoryTable.find(query).sort(new BasicDBObject("Timestamp",-1));
			while (DBcursor.hasNext())
			{
				currHistoryPair = DBcursor.next();
				String username = (String)currHistoryPair.get("UserName");
				long timestamp = (long)currHistoryPair.get("Timestamp");
				Date new_date = new Date(timestamp);
				historyp = new HistoryPair(username, new_date);
				pairs.add(historyp);
			}
			mongoClient.close();
		}catch(Exception e){
			System.out.println(e);
		}
		int arraySize=pairs.size();
		HistoryPair[] historypArray = new HistoryPair[arraySize];
		return pairs.toArray(historypArray);
	}

	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		ArrayList<User> users = new ArrayList<>();
		HistoryPair[] historyArray = getHistoryByItems(title);
		DBCollection  usersTable;
		BasicDBObject query;
		DBCursor dbCursor;
		DBObject dbcurUser;
		User newUser;
		try{
			for(HistoryPair hp: historyArray){
				MongoClient mongoClient = ConnectToDB();
				usersTable = mongoClient.getDB("project3DB").getCollection("Users");
				query = new BasicDBObject();
				query.put("UserName", hp.getCredentials());
				dbCursor = usersTable.find(query);
				while(dbCursor.hasNext()){
					dbcurUser = dbCursor.next();
					String userName = (String)dbcurUser.get("UserName");
					String firstName = (String)dbcurUser.get("FirstName");
					String lastName = (String)dbcurUser.get("LastName");
					newUser = new User(userName, firstName, lastName);
					users.add(newUser);
				}
				mongoClient.close();
			}
		}catch(Exception e){
			System.out.println(e);
		}
		int arraySize=users.size();
		User[] usersArray = new User[arraySize];
		return users.toArray(usersArray);
	}

	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		double result = 0.0;
		HistoryPair[] historyItem1 = getHistoryByItems(title1);
		HistoryPair[] historyItem2 = getHistoryByItems(title2);
		Set<String> usersSet1,usersSet2 ,intersection,union;

		usersSet1 = addingCredentials(historyItem1);
		usersSet2 = addingCredentials(historyItem2);

		intersection = new HashSet<>(usersSet1);
		intersection.retainAll(usersSet2);
		union = new HashSet<>(usersSet1);
		union.addAll(usersSet2);
		int sizeUnion=union.size();
		if (sizeUnion > 0)
			result =(double)intersection.size()/union.size();
		return result;
	}


	private static MongoClient ConnectToDB() {
		Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);
		MongoClientURI uri = new MongoClientURI(
				"mongodb+srv://ass3:asd123456@ass3cluster.qqs8l.mongodb.net/test?retryWrites=true&w=majority");

		MongoClient mongoClient = new MongoClient(uri);
		return mongoClient;
	}


	private static Set<String> addingCredentials(HistoryPair[] hp) {
		Set<String> set = new HashSet<>();
		for(HistoryPair historyp:hp)
			set.add(historyp.getCredentials());
		return set;
	}


	private static boolean checkIfExits(DBCursor dbCursor) {
		boolean result=false;
		while (dbCursor.hasNext()) 
		{ 
			result = true;
			dbCursor.next();
		}
		return result;
	}




}
