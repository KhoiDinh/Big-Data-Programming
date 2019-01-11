package edu.sjsu.cs185C;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.List;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.Optional;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;


/**
 * Consumes messages from the input Kafka topic, calculates averages for a 1-day window,
 * then outputs averages to the output Kafka topic
 *
 * Usage: PM25SparkApp <in-topic> <out-topic> <cg>
 *   <in-topic> is the kafka topic to consume from
 *   <out-topic> is the kafka topic to produce to
 *   <cg> is the consumer group name
 *   <interval> is the number of days per window 
 *
 */

public final class PM25SparkApp {

    // define the class that will be used in the state
    public static class SumState implements Serializable {
        public int sum;         // field to record the total PM25 values of the day
        public int count;       // how many record received for the day
        public int validCount;  // how many record received with the valid PM25 values.
                                // we use this field to calculate the average PM25 value of that day
        public SumState() {
            sum = 0;
            count = 0;
            validCount = 0;
        }
        public int getSum() { return sum; }
        public int getCount() { return count; }
        public int getValidCount() { return validCount; }

        public void setSum(int sum) { this.sum = sum; }
        public void setCount(int sum) { this.count = count; }
        public void setValidCount(int sum) { this.validCount = validCount; }

        @Override
        public String toString() {
            return "sum:" + sum + ", count:" + count + ", validCount:" + validCount;
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: PM25SparkApp <in-topic> <out-topic> <cg> <interval>");
            System.exit(1);
        }

        // set variables from command-line arguments
        String inTopic = args[0];
        final String outTopic = args[1];
        String consumerGroup = args[2];
        int interval = Integer.parseInt(args[3]);

        // define topic to subscribe to
        final Pattern topicPattern = Pattern.compile(inTopic, Pattern.CASE_INSENSITIVE);
    
        // set Kafka client parameters
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        kafkaParams.put("value.deserializer",  "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", consumerGroup);
        kafkaParams.put("auto.offset.reset", "earliest");

        // initialize the streaming context
        JavaStreamingContext jssc = new JavaStreamingContext("local[2]", "PM25SparkApp", new Duration(interval));
        jssc.sparkContext().getConf().set("spark.streaming.stopGracefullyOnShutdown","true");
        jssc.checkpoint("./checkpoints/");

        // pull ConsumerRecords out of the stream
        final JavaInputDStream<ConsumerRecord<Integer, String>> messages = 
                        KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<Integer, String>SubscribePattern(topicPattern, kafkaParams)
                      );

        // pull values out of ConsumerRecords 
        JavaPairDStream<String, String> keyValuePairs =
                messages.mapToPair(new PairFunction<ConsumerRecord<Integer, String>, String, String>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, String> call(ConsumerRecord<Integer, String> record) throws Exception {

                Tuple2<String, String> retTuple = null;
                //TODO: extract the fields year,month,day,hour and the pm2.5 out from the input ConsumerRecord
                //      put them into a Tuple with key as yyyy/mm/dd hh (e.g. 2010/1/1 0)
                //      and value as hour:pm2.5 (which can be NA) 
                //No,year,month,day,hour,pm2.5,DEWP,TEMP,PRES,cbwd,Iws,Is,Ir
                //ConsumerRecord: key:1, value:1,2010,1,1,0,59,-21,-11,1021,NW,1.79,0,0
                //the output Tuple should be 2010/1/1  0:59

                return retTuple;
            }
        });

        // Update the cumulative count function
        Function3<String, Optional<String>, State<SumState>, Tuple2<String, SumState>> mappingFunc =
                new Function3<String, Optional<String>, State<SumState>, Tuple2<String, SumState>>() {
            public Tuple2<String, SumState> call(String datetime, Optional<String> one,
                State<SumState> state) {

                // TODO: get the saved SumState from the state
                // add the current pm25 value into the SumState's sum field, update SumState's count and
                // validCount field.
                // Save the new SumState back to state.
                // return a Tuple with the datetime and the new SumState.

                Tuple2<String, SumState> output = null;
                return output;
              }
            };

        List<Tuple2<String, SumState>> tuples = new ArrayList<Tuple2<String, SumState>>();
        JavaPairRDD<String, SumState> initialRDD = jssc.sparkContext().parallelizePairs(tuples);

        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<String, String, SumState, Tuple2<String, SumState>> stateDstream =
                keyValuePairs.mapWithState(StateSpec.function(mappingFunc).timeout(Durations.seconds(300)).initialState(initialRDD));

        //stateDstream.print();
        stateDstream.foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, SumState>>>() {

            public void call(JavaRDD<Tuple2<String, SumState>> rdd) throws Exception {
                final long totalCount = rdd.count();
                System.out.println("No window get " + totalCount + " records"); 

                if (totalCount <= 0) {
                   return;
                }
                rdd.foreach(new VoidFunction<Tuple2<String, SumState>>() {

                    public void call(Tuple2<String, SumState> tuple) throws Exception {
                        System.out.println("date:" + tuple._1 + ", " +tuple._2);
                        
                        // configure Kafka producer props
                        Properties producerProps = new Properties();
                        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
                        
                        //TODO: if the SumSate's validCount is 24 (we have collected all data from every hour of the day)
                        // create a record with datetime as ProducerRecord key and average pm25 value as ProducerRecord value
                        // send it
                    }
                });
            }
        });

        // start the consumer
        jssc.start();

        // stay in infinite loop until terminated
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            System.out.println("PM25SparkApp is interrupted.");
        }
    }
}

