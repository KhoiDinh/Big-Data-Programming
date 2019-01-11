
package edu.sjsu.cs185C;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.Logger;

public class BinaryTable
{
    private static final Logger log = Logger.getLogger(BinaryTable.class);

    public static void usage()
    {
        System.out.println("Populate the binary table:\n"
                           + "\t edu.sjsu.cs185C.BinaryTable /path/to/inputDataFile /path/to/binaryImdbTableA");
    }

    public static void main( String[] args )
    {
        if (args.length < 2) {
            usage();
            return;
        }

        if (Constants.QualNames.length != Constants.FieldCount) {
            log.error("wrong code: qualifier count is " + Constants.QualNames.length + ", which should be " + Constants.FieldCount);
            return;
        }
        if (Constants.CfNames.length != Constants.FieldCount) {
            log.error("wrong code: qualifier count is " + Constants.CfNames.length + ", which should be " + Constants.FieldCount);
            return;
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", Constants.Quorum);
        conf.set("hbase.zookeeper.property.clientPort", Constants.Port);

        Connection connection = null;

        try {

           connection = ConnectionFactory.createConnection(conf);
           Admin admin = connection.getAdmin();

           String inputFile = args[0];
           TableName binTableName = TableName.valueOf(args[1]);

            if (admin.tableExists(binTableName)) {
                log.info("Deleting " + binTableName);
                if (admin.isTableEnabled(binTableName)) {
                    admin.disableTable(binTableName);
                }
                admin.deleteTable(binTableName);
            }

            HTableDescriptor desc = new HTableDescriptor(binTableName);
            HColumnDescriptor cf1 = new HColumnDescriptor(Constants.CF1);
            desc.addFamily(cf1);
            HColumnDescriptor cf2 = new HColumnDescriptor(Constants.CF2);
            desc.addFamily(cf2);
            HColumnDescriptor cf3 = new HColumnDescriptor(Constants.CF3);
            desc.addFamily(cf3);
            log.info("Creating " + binTableName);
            admin.createTable(desc);

            // TODO: 1. get a Table object from connection.
            //       2. read the input csv file, and for each show with imdbRating > 8.0, generate one HBase Record.
            //          The Column Family and Qualifier for each field are defined in Constants.java
            //          If a field's CfName is "", then that field should not be added to Record.
            //          For the show type fields, only if it is "1", added it to the catalog column family.
            //       For Example, line: titles01/tt0012349,tt0012349,Der Vagabund und das Kind (1921),der vagabund und das kind,http://www.imdb.com/title/tt0012349/,8.4,40550,3240,1921,video.movie,1,0,19,96,85,3,0,0,0,0,0,1,0,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
            //       generates record: 
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
            //
            //       3. use Table.put api to write the generated record to binary table.
            // NOTICE: there are escaped comma "\," in the show title, make sure you handle it correctly when you split the
            //         line to get the fields.
            //       4. close the table.

            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
