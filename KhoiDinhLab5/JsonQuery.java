package edu.sjsu.cs185C;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.SortOrder;

public class JsonQuery{

  public static void usage()
  {
    System.out.println("Run a few queries on the given table:\n"
                         + "\t edu.sjsu.cs185C.Query /path/to/jsonImdbTable");
  }


  public static void main(final String[] args) {

    System.out.println("====== Start Query Application ======");

    if (args.length < 1) {
      usage();
      return;
    }

    // Create an OJAI connection to MapR cluster
    final Connection connection = DriverManager.getConnection("ojai:mapr:");

    // Get an instance of OJAI DocumentStore
    String jsonTableName = args[0];
    final DocumentStore store = connection.getStore(jsonTableName);

    //TODO:
    //  1. for each type in the catalog, create a Query from the json DB
    //      connection, this query finds the 3 shows with highest imdbRating
    //      among all the shows with that type (see the output in the lab
    //      instruction).
    //  2. close the store, connection.
    
    
    //____________________________________________________________
    
    //1
    for(int i=16; i < Constants.QualNames.length; i++)
    {
    	Query query = connection.newQuery() //need to order
    			.select("_id","catalog", "info.title", "info.url", "rating.imdbRating")
    			.where("{\"$eq\": {\"catalog." + Constants.QualNames[i] +"\": \"1\"}}")
    			.orderBy("rating.imdbRating", SortOrder.DESC)
    			//.orderBy("info.title", SortOrder.ASC)
    			//.orderBy("info.duration", SortOrder.ASC)
    			.limit(3)
    			.build();
    	
    	final DocumentStream stream = store.findQuery(query);
    	System.out.println("====== Top 3 shows of " + Constants.QualNames[i]+ " ======");
    	for(final Document userDoc : stream)
    	{
    		System.out.println(userDoc.asJsonString());
    	}
    }
    
    

    //2
    store.close();
    connection.close();
    
    //____________________________________________________________


    System.out.println("====== End Query Application =====");
  }

}
