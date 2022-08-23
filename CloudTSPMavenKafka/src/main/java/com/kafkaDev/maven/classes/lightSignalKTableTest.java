package com.kafkaDev.maven.classes;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.impl.ClientOptionsImpl;

public class lightSignalKTableTest {
	private static String INPUT_TOPIC = "datafeed_lightsignal";
	private static String OUTPUT_TOPIC = "datafeed_lightsignal_master";	
	private static String BOOTSTRAP_SERVER = "localhost:9092";
	public static String KSQL_SERVER_HOST = "localhost";
	public static int KSQL_SERVER_HOST_PORT = 8088;
	private static JSONParser parser = null;
	static {
		parser = new JSONParser();
	}
	
	public static void main(String args[]) throws InterruptedException, ExecutionException {
		Properties streamsConfig = new Properties();
		streamsConfig.put(
				  StreamsConfig.APPLICATION_ID_CONFIG, 
				  "lightsignal-stream-test");
		streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> lightsignalStream = builder.stream(INPUT_TOPIC);
		
		KTable<String,String> lightsignalTable = lightsignalStream.filter((k,v) -> k.contains("9092")).toTable();	
		
		//lightsignalTable.toStream().to(OUTPUT_TOPIC);
		lightsignalTable.toStream().print(Printed.<String, String>toSysOut().withLabel("signals"));
		//lightsignalTable.toStream().print(Printed.<String, String>toSysOut().withLabel("signals"));
		
		//lightsignalStream.groupByKey();
		/*lightsignalStream.foreach(new ForeachAction<String,String>() {
			public void apply(String key, String value) {
				System.out.println(key + " : " + value);
			}
		});*/
		
		Topology topology = builder.build();
		KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
		streams.start();
		
		
		
		
	}
}
