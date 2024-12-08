package org.example;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.glassfish.jersey.jackson.JacksonFeature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


// Inspired by http://stackoverflow.com/questions/14458450/what-to-use-instead-of-org-jboss-resteasy-client-clientrequest
public class TripUpdate {
	static class Task extends TimerTask {
		private Client client;

		// Adapted from http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
		Properties props = new Properties();
		private static int LIMIT = 10;
		private static int OFFSET = 20000000;
		private static String TOPIC = "jycchien_hvfhv";
		KafkaProducer<String, String> producer;


		public TripResponse[] getTripResponse() {
			String url = String.format("https://data.cityofnewyork.us/resource/g6pj-fsah.json?$limit=%s&$offset=%s", LIMIT, OFFSET);
			Invocation.Builder bldr
			  = client.target(url)
					  .request("application/json");
			try {
				return  bldr.get(TripResponse[].class);
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
			return null;  // Sometimes the web service fails due to network problems. Just let it try again
		}


		public Task() {
			client = ClientBuilder.newClient();
			// enable POJO mapping using Jackson - see
			// https://jersey.java.net/documentation/latest/user-guide.html#json.jackson
			client.register(JacksonFeature.class); 
			props.put("bootstrap.servers", bootstrapServers);
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			producer = new KafkaProducer<>(props);
		}

		@Override
		public void run() {

			TripResponse[] tripData = getTripResponse();

			if(tripData == null || tripData.length == 0){
				System.out.println("No data received or end of data reached.");
				return;
			}

			ObjectMapper mapper = new ObjectMapper();
			System.out.println("running");

			for (TripResponse trip : tripData) {
				ProducerRecord<String, String> data;
				try {
					data = new ProducerRecord<String, String> (
							TOPIC,
							mapper.writeValueAsString(trip));
					producer.send(data);
				} catch (JsonProcessingException e) {
					// System.err.println(e.getMessage());
					e.printStackTrace();
				}
			}

			// Increment offset for the next API call
			OFFSET += LIMIT;

		}



	}

	static String bootstrapServers = new String("$KAFKABROKERS");

	public static void main(String[] args) {
		if(args.length > 0)  // This lets us run on the cluster with a different kafka
			bootstrapServers = args[0];
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new Task(), 0, 60*1000);
	}
}

