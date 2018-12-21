package edu.sjsu.cs185C;

import org.apache.kafka.clients.consumer.*;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class PM25Consumer {

    // Declare a new consumer.
    public static KafkaConsumer<String, Integer> consumer;

    public static void main(String[] args) throws IOException {
        // check command-line arguments
        if(args.length != 2) {
            System.err.println("usage: PM25Consumer <input-topic> <cg>");
            System.exit(1);
        }
        String inputTopic = args[0];
        String groupId = args[1];
        
        configureConsumer(groupId);

        // Subscribe to the topic.
        List<String> topics = new ArrayList<String>();
        topics.add(inputTopic);
        consumer.subscribe(topics);

        // Set the timeout interval for requests for unread messages.
        long pollTimeOut = 1000;

        // this table is used to keep the date where the pm25 value is highest in that year.
        Map<String,ConsumerRecord<String,Integer>> highestPMWeek = new HashMap<String, ConsumerRecord<String, Integer>>();

        ConsumerRecords<String, Integer> consumerRecords=null;
        ConsumerRecord<String, Integer> record=null;
        Iterator<ConsumerRecord<String, Integer>> iterator=null;

        int waitedTime = 0; //seconds
        while(true) {
            // Request unread messages from the topic.
            int count=0;
            consumerRecords = consumer.poll(pollTimeOut);
            iterator = consumerRecords.iterator();
            while (iterator.hasNext()) {
                //TODO: if the current one has a higher pm25 value, put/replace it into highestPMWeek
                record = iterator.next();
                
                //-----------------------------------
                
                //System.out.println("Consumed Record: " + "HI");
                String year = record.key().split("/")[0];
                if(highestPMWeek.containsKey(year) == false)
                {
                	highestPMWeek.put(year, record);
                }
                else if(highestPMWeek.get(year).value() < record.value())
                {
                	highestPMWeek.put(year, record);
                }
                else
                {
                	continue;
                }
                //extract year and use that as key, replace year key with new value if new value higher
                
                //-----------------------------------
                count++;
            }
            if(count == 0) {
                waitedTime += 1;
            } else {
                waitedTime = 0; //reset when we get new data
            }
            if (waitedTime > 30) {  // stop if no data received for 2 minutes
                break;
            }
        } // end of while(true)

        //TODO: print each year with its date and its highest pm25 value
        TreeMap<String,ConsumerRecord<String,Integer>> sorted = new TreeMap<String,ConsumerRecord<String,Integer>>();
        sorted.putAll(highestPMWeek);
        sorted.forEach((key,value) -> System.out.println(key + " " + value.key() + " " + value.value()));
        
    }

    public static void configureConsumer(String groupId) {
        Properties props = new Properties();
        props.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest");

        consumer = new KafkaConsumer<String, Integer>(props);
    }
}

