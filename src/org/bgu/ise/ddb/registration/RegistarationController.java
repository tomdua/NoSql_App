/**
 * 
 */
package org.bgu.ise.ddb.registration;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;

//import java.util.Collection;
//import java.util.*;
import com.mongodb.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;

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
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{


	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		MongoClient mongoClient;
		DBCollection dbcollection;
		BasicDBObject newUser;

		try{
			if(isExistUser(username)){
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
			}
			else{
				mongoClient = ConnectToDB();
				dbcollection = mongoClient.getDB("project3DB").getCollection("Users");
				newUser = new BasicDBObject();
				newUser.put("UserName", username);
				newUser.put("Password", password);
				newUser.put("FirstName", firstName);
				newUser.put("LastName", lastName);
				newUser.put("RegistrationDate", new Date());
				dbcollection.insert(newUser);
				mongoClient.close();

				HttpStatus status = HttpStatus.OK;
				response.setStatus(status.value());
			}
		}catch(Exception e){
			System.out.println(e.getStackTrace());
		}

	}

	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean exitUser = false;
		MongoClient mongoClient;
		DBCollection  dbCollection;
		BasicDBObject query;
		DBCursor dbCursor;
		try {
			mongoClient = ConnectToDB();
			dbCollection = mongoClient.getDB("project3DB").getCollection("Users");
			query = new BasicDBObject();
			query.put("UserName", username);
			dbCursor = dbCollection.find(query);
			exitUser = checkIfExits(dbCursor);
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e.getStackTrace());
		}
		return exitUser;
	}

	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean validUser = false;
		BasicDBObject query;
		DBCursor dBcursor;
		try {
			MongoClient mongoClient = ConnectToDB();
			DBCollection  collection = mongoClient.getDB("project3DB").getCollection("Users");
			query = new BasicDBObject();
			query.put("UserName", username);
			query.put("Password", password);
			dBcursor = collection.find(query);
			validUser = checkIfExits(dBcursor);
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e.getStackTrace());
		}

		return validUser;

	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int registUserNum = 0;
		Date curDate,dateToCheck,regDate;
		DBObject DBcurUser;
		DBCursor dbcursor;
		DBCollection  collection;
		MongoClient mongoClient;
		try {
			mongoClient = ConnectToDB();
			collection = mongoClient.getDB("project3DB").getCollection("Users");
			dbcursor = collection.find();
			int timeDate=  (1000 * 60 * 60 *24)*days;
			curDate = new Date();
			dateToCheck = new Date(curDate.getTime() - timeDate);
			while (dbcursor.hasNext()) 
			{ 
				DBcurUser = dbcursor.next();
				regDate = (Date) DBcurUser.get("RegistrationDate");
				if(regDate.getTime() > dateToCheck.getTime())
					registUserNum++;
			}
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e.getStackTrace());
		}
		return registUserNum;
	}

	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public static User[] getAllUsers(){
		DBObject DBcurUser;
		String userName,password,lastName,firstName;
		User user;
		List<User> listUsers = new ArrayList<User>();
		try {
			MongoClient mongoClient = ConnectToDB();
			DBCollection dbCollection = mongoClient.getDB("project3DB").getCollection("Users");
			DBCursor dbcursor = dbCollection.find();
			while (dbcursor.hasNext()) 
			{ 
				DBcurUser = dbcursor.next();
				userName = (String)DBcurUser.get("UserName");
				password = (String)DBcurUser.get("Password");
				firstName = (String)DBcurUser.get("FirstName");
				lastName = (String)DBcurUser.get("LastName");
				user = new User(userName, password, firstName, lastName);
				listUsers.add(user);
			}
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e.getStackTrace());
		}
		int numOfUsers = listUsers.size();
		User [] userArray =new User[numOfUsers];
		for(int i=listUsers.size()-1,j=0;i>-1;i--) {
			userArray[j]=listUsers.get(i);
			j++;
		}
		return userArray;
	}


	private static MongoClient ConnectToDB() {
		Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);
		MongoClientURI uri = new MongoClientURI(
				"mongodb+srv://ass3:asd123456@ass3cluster.qqs8l.mongodb.net/test?retryWrites=true&w=majority");

		MongoClient mongoClient = new MongoClient(uri);
		return mongoClient;
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
