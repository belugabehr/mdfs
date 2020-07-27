package io.github.belugabehr.mdfs.table.wal.kafka;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import com.google.protobuf.InvalidProtocolBufferException;

import io.github.belugabehr.mdfs.api.FileOperations.FileOperationRequest;
import io.github.belugabehr.mdfs.table.file.FileOperationHandler;
import io.github.belugabehr.mdfs.tablenode.TableNode.WalEntry;

public class KafkaWalListener {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaWalListener.class);

	private final FileOperationHandler operationHandler;

	public KafkaWalListener(FileOperationHandler handler) {
		this.operationHandler = Objects.requireNonNull(handler);
	}

	@KafkaListener(id = "walListener", topics = "mdfs.wal")
	public void listen(@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp, final WalEntry data) {
		LOG.info("RECEIVED FROM KAFKA: [version:{}] {}", timestamp, data);
		try {
			final FileOperationRequest request = data.getFileOperationRequest().unpack(FileOperationRequest.class);
			this.operationHandler.handle(timestamp, request);
		} catch (InvalidProtocolBufferException e) {
			LOG.error("Error", e);
		}
	}

}
