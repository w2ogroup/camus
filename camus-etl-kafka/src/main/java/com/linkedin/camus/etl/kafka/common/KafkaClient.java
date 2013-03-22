package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import kafka.api.PartitionFetchInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.SimpleConsumer;

/**
 * 
 * One stop client to everything related to Kafka
 * 
 */

public class KafkaClient {
	public static HashMap<String, List<EtlRequest>> partitionInfo = new HashMap<String, List<EtlRequest>>();
	private static String KAFKA_CLIENT_NAME = "hadoop-etl-test";
	private static String KAFKA_HOST_URL = "eat1-app263.corp.linkedin.com";
	private static int KAFKA_HOST_PORT = 9092;
	private static int KAFKA_BUFFER_SIZE = 1024 * 1024;
	private static int KAFKA_TIMEOUT_VALUE = 30000;
	private static int KAFKA_FETCH_REQUEST_CORRELATIONID = 23;
	private static int KAFKA_FETCH_REQUEST_MAX_WAIT = 1000;
	private static int KAFKA_FETCH_REQUEST_MIN_BYTES = 4096;

	public static HashMap<String, List<EtlRequest>> createEtlRequests(
			List<TopicMetadata> topicMetadataList,
			ArrayList<String> topicsToDiscard) throws IOException

	{
		for (TopicMetadata topicMetadata : topicMetadataList) {
			if (topicsToDiscard.contains(topicMetadata.topic())) {
				System.out.println("Discarding topic : "
						+ topicMetadata.topic());
				continue;
			}
			List<PartitionMetadata> partitionsMetadata = topicMetadata
					.partitionsMetadata();
			List<EtlRequest> tempEtlRequests = new ArrayList<EtlRequest>();
			for (PartitionMetadata partitionMetadata : partitionsMetadata) {
				if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
					continue;
				} else {
					EtlRequest etlRequest;
					try {
						etlRequest = new EtlRequest(topicMetadata.topic(),
								Integer.toString(partitionMetadata.leader()
										.id()),
								partitionMetadata.partitionId(), new URI(
										"tcp://"
												+ partitionMetadata.leader()
														.getConnectionString()));
						tempEtlRequests.add(etlRequest);
					} catch (URISyntaxException e) {
						System.out
								.println("Error in generating the broker URI after loading the Kafka Metadata.");
						e.printStackTrace();
					}
				}
			}
			if (tempEtlRequests.size() != 0) {
				partitionInfo.put(topicMetadata.topic(), tempEtlRequests);
			}
		}
		return partitionInfo;
	}

	public FetchResponse getFetchRequests(SimpleConsumer simpleConsumer,
			String topicName, int partitionId, int leaderId, long offset,
			int fetchBufferSize) {
		PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(offset,
				fetchBufferSize);
		HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
		TopicAndPartition topicAndPartition = new TopicAndPartition(topicName,
				partitionId);
		fetchInfo.put(topicAndPartition, partitionFetchInfo);
		FetchRequest fetchRequest = new FetchRequest(
				getKafkaFetchRequestCorrelationId(), getKafkaClientName(),
				getKafkaFetchRequestMaxWait(), getKafkaFetchRequestMinBytes(),
				fetchInfo);
		FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
		return fetchResponse;
	}

	// Set all kafka related parameters from the configuration file
	public static void setAllKafkaParameters(JobContext job) {
		Configuration config = job.getConfiguration();
		KAFKA_BUFFER_SIZE = config.getInt("kafka.buffer.size", 1024 * 1024);
		KAFKA_CLIENT_NAME = config.get("kafka.client.name");
		KAFKA_FETCH_REQUEST_CORRELATIONID = config.getInt(
				"kafka.fetch.request.correlationid", -1);
		KAFKA_FETCH_REQUEST_MAX_WAIT = config.getInt(
				"kafka.fetch.request.max.wait", 0);
		KAFKA_FETCH_REQUEST_MIN_BYTES = config.getInt(
				"kafka.fetch.request.min.bytes", 0);
		KAFKA_HOST_URL = config.get("kafka.host.url");
		KAFKA_HOST_PORT = config.getInt("kafka.host.port", -1);
		KAFKA_TIMEOUT_VALUE = config.getInt("kafka.timeout.value", 30000);
//		System.out
//				.println("These are the value of the parameters set for Kafka: ");
//		System.out.println("KAFKA_BUFFER_SIZE:" + KAFKA_BUFFER_SIZE);
//		System.out.println("KAFKA_CLIENT_NAME:" + KAFKA_CLIENT_NAME);
//		System.out.println("KAFKA_TIMEOUT_VALUE:" + KAFKA_TIMEOUT_VALUE);
//		System.out.println("KAFKA_FETCH_REQUEST_CORRELATIONID:"
//				+ KAFKA_FETCH_REQUEST_CORRELATIONID);
//		System.out.println("KAFKA_FETCH_REQUEST_MAX_WAIT:"
//				+ KAFKA_FETCH_REQUEST_MAX_WAIT);
//		System.out.println("KAFKA_FETCH_REQUEST_MIN_BYTES:"
//				+ KAFKA_FETCH_REQUEST_MIN_BYTES);
//		System.out.println("KAFKA_HOST_URL:" + KAFKA_HOST_URL);
//		System.out.println("KAFKA_HOST_PORT:" + KAFKA_HOST_PORT);
	}

	// This needs to be place somewhere where all can be read

	public static String getKafkaClientName() {
		return KAFKA_CLIENT_NAME;
	}

	public static String getKafkaHostURL() {
		return KAFKA_HOST_URL;
	}

	public static int getKafkaHostPort() {
		return KAFKA_HOST_PORT;
	}

	public static int getKafkaTimeoutValue() {
		return KAFKA_TIMEOUT_VALUE;
	}

	public static int getKafkaBufferSize() {
		return KAFKA_BUFFER_SIZE;
	}

	public static int getKafkaFetchRequestCorrelationId() {
		return KAFKA_FETCH_REQUEST_CORRELATIONID;
	}

	public static int getKafkaFetchRequestMaxWait() {
		return KAFKA_FETCH_REQUEST_MAX_WAIT;
	}

	public static int getKafkaFetchRequestMinBytes() {
		return KAFKA_FETCH_REQUEST_MIN_BYTES;
	}

}