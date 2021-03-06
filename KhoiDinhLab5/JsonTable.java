package edu.sjsu.cs185C;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import org.ojai.Document;
import org.ojai.DocumentBuilder;
import org.ojai.Value;
import org.ojai.json.Json;
import org.ojai.json.impl.JsonDocumentBuilder;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import com.mapr.db.MapRDB;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;


public class JsonTable
{
    private static final Logger log = Logger.getLogger(JsonTable.class);

    public static void usage()
    {
        System.out.println("Populate the json table from the binary table, and run a few queries:\n"
                           + "\t edu.sjsu.cs185C.JsonTable /path/to/binaryImdbTable /path/to/jsonImdbTable");
    }

    @SuppressWarnings("deprecation")
	public static void main( String[] args )
    {

        if (args.length < 2) {
            usage();
            return;
        }

        Configuration binDBConf = HBaseConfiguration.create();
        binDBConf.set("hbase.zookeeper.quorum", Constants.Quorum);
        binDBConf.set("hbase.zookeeper.property.clientPort", Constants.Port);

        org.apache.hadoop.hbase.client.Connection binDBConn = null;
        org.ojai.store.Connection jsonDBConn = null;

        try {

           binDBConn = ConnectionFactory.createConnection(binDBConf);
           org.apache.hadoop.hbase.client.Admin binAdmin = binDBConn.getAdmin();

           TableName binTableName = TableName.valueOf(args[0]);
           String jsonTableName = args[1];

            if (!binAdmin.tableExists(binTableName)) {
                log.error(binTableName + " not exists!");
                return;
            }
            Table binTable = binDBConn.getTable(binTableName);

            if (MapRDB.tableExists(jsonTableName)) {
                log.info("deleting " + jsonTableName );
                MapRDB.deleteTable(jsonTableName);
            }
            log.info("creating " + jsonTableName );
            MapRDB.createTable(jsonTableName);

            jsonDBConn = DriverManager.getConnection("ojai:mapr:");

            //TODO: 
            // 1. get an instance of DocumentStore from the Json DB connection
            // 2. create a scanner for the binary table
            // 3. for each HBase Result returned from the scanner, use DocumentBuilder to
            //    build a json document from it.
            //    For example, a HBase row tt0012349
            //       COLUMN                            CELL                                                                                            
            //       catalog:Comedy                   timestamp=1541979430814, value=1                                                                
            //       catalog:Drama                    timestamp=1541979430814, value=1                                                                
            //       catalog:Family                   timestamp=1541979430814, value=1                                                                
            //       info:duration                    timestamp=1541979430814, value=3240                                                             
            //       info:title                       timestamp=1541979430814, value=Der Vagabund und das Kind (1921)                                 
            //       info:url                         timestamp=1541979430814, value=http://www.imdb.com/title/tt0012349/                             
            //       info:year                        timestamp=1541979430814, value=1921                                                             
            //       rating:imdbRating                timestamp=1541979430814, value=8.4                                                              
            //       rating:nrOfNominations           timestamp=1541979430814, value=0                                                                
            //       rating:nrOfWins                  timestamp=1541979430814, value=1                                                                
            //       rating:ratingCount               timestamp=1541979430814, value=40550        

            // will generate document
            // {
            //   "_id" : "tt0012349",
            //   "catalog" : {
            //     "Comedy" : "1",
            //     "Drama" : "1",
            //     "Family" : "1"
            //   },
            //   "info" : {
            //     "duration" : "3240",
            //     "title" : "Der Vagabund und das Kind (1921)",
            //     "url" : "http://www.imdb.com/title/tt0012349/",
            //     "year" : "1921"
            //   },
            //   "rating" : {
            //     "imdbRating" : "8.4",
            //     "nrOfNominations" : "0",
            //     "nrOfWins" : "1",
            //     "ratingCount" : "40550"
            //   }
            // }

            // NOTICE: HBase support multi-versions, even though we create the table with only one version,
            //         the scanner results APIs are written for multi-versions.

            // 4. insert the document to the json store.
            // 5. close scanner, binary table, json table and the connections.
            
            
            //____________________________________________________________
            
            //1
            DocumentStore getDS = jsonDBConn.getStore(jsonTableName);
            
            
            
            //2
           
            Scan scanBT = new Scan();
            ResultScanner scanner = binTable.getScanner(scanBT);
            
            
            //3
            /*for (Result r : scanner) 
            {
            	Document builder = Json.newDocument();
                
            	builder.setId(new String(r.getRow()));
            	
            	
                for (Cell keyValue : r.listCells()) 
                {
                	
                	builder.set(new String(keyValue.getFamily()) + "." + new String(keyValue.getQualifier()), new String(keyValue.getValue()));
                	
                	//family: catalog	qualifier: comedy	value: 1
                	
                }

                //4
                getDS.insert(builder);
            }*/
            
            for(Result r: scanner)
            {
            	DocumentBuilder builder = Json.newDocumentBuilder();
            	
            	builder.addNewMap();
            	builder.put("_id", new String(r.getRow()));

            	
            	
            	String previous = "";
            	for(Cell keyValue : r.listCells())
            	{
            		
            		if(!previous.equals(new String(keyValue.getFamily())))
            		{
            			if(!previous.equals(""))
            			{
            				builder.endMap();
            			}
            			builder.putNewMap(new String(keyValue.getFamily()));
            			builder.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
            			previous = new String(keyValue.getFamily());
            		}
            		else
            		{
            			builder.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
            		}
            	}
            	
            	builder.endMap();
            	builder.endMap();
            	

            	Document doc = builder.getDocument();
            	getDS.insert(doc);
            }
            	
            	
            	
            
            
            //5
            scanner.close();
            binTable.close();
            binDBConn.close();
            
            
            //____________________________________________________________

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
