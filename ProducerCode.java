package simple.kafka.tut1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class producerdemowithcallback {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(producerdemowithcallback.class);


        // Create Producer Properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.19.40:9092,172.31.30.79:9092,172.31.16.205:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // create the Producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        InputStream inputStream = producerdemowithcallback.class.getResourceAsStream("data.json");

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String jsonString = null;
        try {
             jsonString = bufferedReader.readLine();
        }catch(IOException e)
        {}

        Map<String, String> map = new HashMap<>();
        JSONObject jObject = new JSONObject(jsonString);
        Iterator<?> keys = jObject.keys();

        while (keys.hasNext()) {
            String key = (String) keys.next();
            String value = jObject.getJSONObject(key).toString();
            map.put(key, value);

        }

        for (String key : map.keySet()) {
            System.out.println("key: " + key + " Value:" + map.get(key));

            final ProducerRecord<String,String> record =
                    new ProducerRecord<String, String>("warehouse", map.get(key));
            // send data asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {

                    // executes everytime a record is sent - success or exception
                    if (exception == null) {
//                    // Record sent
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while producing", exception );
                    }
                }
            });

        }

        producer.flush();
        producer.close();
        }

        // create a Producer Record

        // Flush and close



    }