package io.github.belugabehr.mdfs.table.wal.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import io.github.belugabehr.mdfs.table.file.FileOperationHandler;
import io.github.belugabehr.mdfs.table.wal.WalEntryDeserializer;
import io.github.belugabehr.mdfs.table.wal.WalEntrySerializer;
import io.github.belugabehr.mdfs.tablenode.TableNode.WalEntry;

@Configuration
@EnableKafka
@ComponentScan(basePackages = "io.github.belugabehr.mdfs.table.wal.kafka")
public class KafkaConfig {

	private static final int PORT = 32768;

	@Bean
	public ProducerFactory<String, WalEntry> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.1:" + PORT);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WalEntrySerializer.class);
		// See https://kafka.apache.org/documentation/#producerconfigs for more
		// properties
		return props;
	}

	@Bean
	public KafkaTemplate<String, WalEntry> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory(), true);
	}

	@Bean
	public KafkaAdmin admin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.1:" + PORT);
		return new KafkaAdmin(configs);
	}

	@Bean
	public NewTopic topic1(KafkaAdmin kafkaAdmin) {
		AdminClient.create(kafkaAdmin.getConfig()).deleteTopics(Collections.singletonList("mdfs.wal"));
		return TopicBuilder.name("mdfs.wal")
				.config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.toString())
				.partitions(1).replicas(1).build();
	}

	@Bean
	public ConsumerFactory<String, WalEntry> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, WalEntry> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, WalEntry> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}

	@Bean
	public KafkaWalListener walListener(FileOperationHandler handler) {
		return new KafkaWalListener(handler);
	}

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.1:" + PORT);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WalEntryDeserializer.class);

		// Random UUID so that it will start from the beginning of all time every time
		// and that it will keep its own offset in the topic
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "mdfs-listener");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		return props;
	}

}
