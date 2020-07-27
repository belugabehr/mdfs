package io.github.belugabehr.mdfs.table;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

public class Initializer {

	private static final Logger LOG = LoggerFactory.getLogger(Initializer.class);

	private ConsumerFactory<Integer, String> consumerFactory;

	public void init() throws IOException {

		DirectoryStream.Filter<java.nio.file.Path> filter = file -> {
			return file.toString().endsWith(".halb");
		};

		try (DirectoryStream<java.nio.file.Path> directoryStream = Files.newDirectoryStream(Paths.get("/tmp/rfiles"),
				filter)) {

			PrimitiveType type = Types.required(PrimitiveTypeName.INT64).named("seq");
			Statistics<?> stats = Statistics.getBuilderForReading(type).build();

			for (java.nio.file.Path path : directoryStream) {
				ParquetFileReader reader = ParquetFileReader
						.open(null);

				for (final BlockMetaData rowGroup : reader.getRowGroups()) {
					for (final ColumnChunkMetaData column : rowGroup.getColumns()) {
						if (type.equals(column.getPrimitiveType())) {
							stats.mergeStatistics(column.getStatistics());
						}
					}
				}
			}
			LOG.warn("MAX SEQUENCE: {}", stats.genericGetMax());
		} catch (IOException ex) {
		}

//		System.exit(-1);

		Consumer<Integer, String> consumer = this.consumerFactory.createConsumer();
		consumer.subscribe(Collections.singleton("fs.wal"));

		boolean drained = false;
		do {
			ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
			drained = records.isEmpty();
			if (!drained) {
				for (final ConsumerRecord<Integer, String> record : records) {
					LOG.info("Read: {}", record.value());
				}
			}
		} while (!drained);

		consumer.close();

		LOG.info("Initialized");
	}

}
