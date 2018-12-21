package edu.sjsu.cs185C;

/**
 * This producer reads from our data file, and writes record one at a time
 * to MapR stream.
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class PM25Producer{
    // Set the stream and topic to publish to.
    public static String topic;

    // Declare a new producer.
    public static KafkaProducer<Integer, String> producer;
        
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        // check command-line args
        if(args.length != 3) {
            System.err.println("usage: PM25Producer <input-file>  <output-topic> <sleep-time>");
            System.exit(1);
        }
        String inputFile = args[0];
        String outputTopic = args[1];
        long sleepTime = Long.parseLong(args[2]);

        // configure the producer
        configureProducer();
        BufferedReader reader=null;

        try {
                reader = new BufferedReader(new FileReader(new File(inputFile)));  
                String record;
                while((record=reader.readLine())!=null) {
                    //skip the first line
                    if(record.startsWith("No,")) {
                        continue;
                    }

                    //line example:
                    //No,year,month,day,hour,pm2.5,DEWP,TEMP,PRES,cbwd,Iws,Is,Ir
                    //1,2010,1,1,0,NA,-21,-11,1021,NW,1.79,0,0

                    // TODO: create a ProducerRecord with "No" as key, and entire record as value
                    //       send the ProducerRecord to topic
                    //_______________________________________________________________________________________
                    if(record.split(",").length == 13)
                    {
	                    ProducerRecord<Integer,String> rec = new ProducerRecord<Integer, String>(outputTopic, Integer.parseInt(record.split(",")[0]), record);
	                    //System.out.println(rec.toString());
	                    producer.send(rec);
                    }
                    
                    //_______________________________________________________________________________________
                    // sleep a short interval to throttle the IO.
                    Thread.sleep(sleepTime);
                }
        } catch(IOException io){
                io.printStackTrace();
        }
        reader.close();
        producer.close();
    }

    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer,String>(props);
    }
}
