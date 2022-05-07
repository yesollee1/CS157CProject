package cs157cproject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MovieInfo {

	public static void main(String[] args) {
		Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
		mongoLogger.setLevel(Level.SEVERE); 
		
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		MongoDatabase db = mongoClient.getDatabase("movieinfo");
		MongoCollection<Document> movies = db.getCollection("movies");
		MongoCollection<Document> genomeScores = db.getCollection("genome-scores");
		MongoCollection<Document> genomeTags = db.getCollection("genome-tags");
		MongoCollection<Document> links = db.getCollection("links");
		MongoCollection<Document> ratings = db.getCollection("ratings");
		MongoCollection<Document> tags = db.getCollection("tags");
		
//		try {
//			//MongoClient mongoClient = new MongoClient(new MongoClientURI((""));
//			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
//			MongoDatabase db = mongoClient.getDatabase("movieinfo");
//			System.out.println("Connected to Database");
//			System.out.println("Server is ready");
//
//		}catch(Exception e){
//			System.out.println(e);
//		}

		MongoCursor<Document> cur = movies.find().iterator();
		try{
	    	while(cur.hasNext()) {
	    		Document doc = cur.next();
	    		ArrayList moviesList = new ArrayList<> (doc.values());
	    		
	    		System.out.println(cur.next().toJson());
	    	}
	    }finally {
	    	cur.close();
	    }
	    
//	    try(MongoCursor<Document> cur = movies.find().iterator()){
//	    	while(cur.hasNext()) {
//	    		Document doc = cur.next();
//	    		ArrayList moviesList = new ArrayList<> (doc.values());
//	    		
//	    		System.out.printf("%s: %s%n", moviesList.get(1), moviesList.get(2));
//	    	}
//	    }
	    
	    Scanner input = new Scanner(System.in);
	    System.out.println("Options: \n"
	    		+ "1: Find a movie based on movieId\n"
	    		+ "2: Find a movie based on title\n"
	    		+ "3: Find all the ratings by a specific user\n"
	    		+ "4: Find tags with a certain keyword\n"
	    		+ "5: Find a link with the movie name\n"
	    		+ "6: Add a new movie\n"
	    		+ "7: Find tags by a certain user\n"
	    		+ "8: Find all genres in the collection\n"
	    		+ "9: Update the title of a movie\n"
	    		+ "10: Find a movie by genre\n"
	    		+ "11: Delete all movies where the rating is less than 1\n"
	    		+ "12: Add a new rating for a movie\n"
	    		+ "13: Find 10 movieIDs with a rating less than 3\n"
	    		+ "14: Delete a rating with a specific movieID\n"
	    		+ "15: Find ratings with movieID less than a specific number\n"
	    		+ "16: Quit program\n");
	    System.out.println("Enter option: ");
	    int option = input.nextInt();
	    while(option != 16) {
	    	if(option == 1)
	    		option1(movies);
	    	else if(option == 2)
	    		option2(movies);
	    	else if(option == 3)
	    		option3(ratings);
	    	else if(option == 4)
	    		option4(tags);
	    	else if(option == 5)
	    		option5(links);
	    	else if(option == 6)
	    		option6(movies);
	    	else if(option == 7)
	    		option7(tags);
	    	else if(option == 8)
	    		option8(movies);
	    	else if(option == 9)
	    		option9(movies);
	    	else if(option == 10)
	    		option10(movies);
	    	else if(option == 11)
	    		option11(ratings);
	    	else if(option == 12)
	    		option12(ratings);
	    	else if(option == 13)
	    		option13(ratings);
	    	else if(option == 14)
	    		option14(ratings);
	    	else
	    		option15(ratings);
	    	System.out.println("Enter option: ");
		    option = input.nextInt();
	    }
	}
	
	//finds a movie based on movieId
	public static void option1(MongoCollection<Document> movies) {
		System.out.println("Enter a movieId:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		Document id = new Document("movieId", input);
		
		MongoCursor<Document> cursor = movies.find(id).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	//finds a movie based on title
	public static void option2(MongoCollection<Document> movies) {
		System.out.println("Enter a title:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		Document title = new Document("title", input);
		
		MongoCursor<Document> cursor = movies.find(title).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}

	//3: Find all the ratings by a specific user
	public static void option3(MongoCollection<Document> ratings) {
		System.out.println("Enter a userId:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		Document rating = new Document("userId", input);
		
		MongoCursor<Document> cursor = ratings.find(rating).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	

	//4: Find tags with a certain keyword
	public static void option4(MongoCollection<Document> tags) {
		System.out.println("Enter a tag keyword (e.g. adventure):");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		Document tag = new Document("tag", input);
		
		MongoCursor<Document> cursor = tags.find(tag).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	//5: User searches imdb links (Ids) with movieId
	public static void option5(MongoCollection<Document> links) {
		System.out.println("Enter a movieId to search the imdbId:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		BasicDBObject query = new BasicDBObject();
		query.put("movieId", input);
		query.append("_id", 0).append("movieId", 1).append("imdbId", 1).append("tmdbId", 0);
		MongoCursor<Document> cursor = links.find(query).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	

	//6: Add a new movie
	public static void option6(MongoCollection<Document> movies) {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the id of the movie:");
		String id = sc.nextLine();
		System.out.println("Enter the title of the movie:");
		String title = sc.nextLine();
		System.out.println("Enter the genre of the movie:");
		String genre = sc.nextLine();
		
		Document newMovie = new Document("movieId", id).append("title", title).append("genres", genre);
		movies.insertOne(newMovie);
		
		MongoCursor<Document> cursor = movies.find(newMovie).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}

	//7: Find tags by a certain user
	public static void option7(MongoCollection<Document> tags) {
		System.out.println("Enter userId:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		Document tag = new Document("userId", input);
		
		MongoCursor<Document> cursor = tags.find(tag).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	//8: Find all genres in the collection
	public static void option8(MongoCollection<Document> movies) {
		BasicDBObject fields = new BasicDBObject();
		fields.put("_id", 0);
		fields.put("movieId", 0);
		fields.put("title", 0);
		fields.put("genres", 1);

		MongoCursor<Document> cursor = movies.find(fields).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	//9: Update the title of a movie
	public static void option9(MongoCollection<Document> movies) {
		System.out.println("Enter the title of the movie:");
		Scanner sc = new Scanner(System.in);
		String title = sc.nextLine();
		System.out.println("Enter the new title of the movie:");
		String newTitle = sc.nextLine();
		
		BasicDBObject query = new BasicDBObject();
		query.put("title", title);
		
		BasicDBObject newDocument = new BasicDBObject();
		newDocument.put("title", newTitle);
		
		BasicDBObject updateObject = new BasicDBObject();
		updateObject.put("$set", newDocument);
		
		movies.updateOne(query, updateObject);
	}
	
	//10: Find a movie by genre
	public static void option10(MongoCollection<Document> movies) {
		System.out.println("Enter a genre:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		Document genre = new Document("genre", input);
		
		MongoCursor<Document> cursor = movies.find(genre).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	//11: Delete all movies where the rating is less than 1
	public static void option11(MongoCollection<Document> ratings) {
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("rating", 1);
		
		MongoCursor<Document> cursor = ratings.find(searchQuery).iterator();
		while(cursor.hasNext()) {
			ratings.deleteOne(cursor.next());
		}
	}
	
	//12: Add a new rating for a movie
	public static void option12(MongoCollection<Document> ratings) {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter a userId:");
		String userId = sc.nextLine();
		System.out.println("Enter a movieId:");
		String movieId = sc.nextLine();
		System.out.println("Enter a rating:");
		String rating = sc.nextLine();
		
		Document newRating = new Document("userId", userId).append("movieId", movieId).append("rating", rating);
		ratings.insertOne(newRating);
		
		MongoCursor<Document> cursor = ratings.find(newRating).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	//13: Find 10 ratings with rating less than 3
	public static void option13(MongoCollection<Document> ratings) {
		System.out.println("Enter a rating:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		BasicDBObject query = new BasicDBObject("rating",new BasicDBObject("$lt", input));
		MongoCursor<Document> cursor = ratings.find(query).limit(10).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	//14: Delete a rating with a specific movieID
	public static void option14(MongoCollection<Document> ratings) {
		System.out.println("Enter a movieId:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		Document rating = new Document("movieId", input);
		
		MongoCursor<Document> cursor = ratings.find(rating).iterator();
		while(cursor.hasNext()) {
			ratings.deleteOne(cursor.next());
		}
		
		MongoCursor<Document> cursorCheck = ratings.find(rating).iterator();
		if(cursorCheck.hasNext()) {
			System.out.println("Error deleting document");
		}
		else {
			System.out.println("Deleted successfully");
		}
	}
	
	//Find ratings with movieID less than a specific number
	public static void option15(MongoCollection<Document> ratings) {
		System.out.println("Enter a movieId:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		BasicDBObject query = new BasicDBObject("movieId",new BasicDBObject("$lt", input));
		MongoCursor<Document> cursor = ratings.find(query).iterator();
		while(cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
}
