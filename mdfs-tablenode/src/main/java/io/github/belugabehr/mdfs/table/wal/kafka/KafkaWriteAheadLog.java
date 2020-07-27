package io.github.belugabehr.mdfs.table.wal.kafka;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.google.protobuf.Any;

import io.github.belugabehr.mdfs.api.FileOperations.FileOperationRequest;
import io.github.belugabehr.mdfs.table.wal.WriteAheadLog;
import io.github.belugabehr.mdfs.tablenode.TableNode.WalEntry;

@Component
public class KafkaWriteAheadLog implements WriteAheadLog<Long, FileOperationRequest> {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaWriteAheadLog.class);

	@Autowired
	private KafkaTemplate<String, WalEntry> kafkaTemplate;

	@Override
	public synchronized Long add(FileOperationRequest entry) throws Exception {
		try {
			LOG.info("Writing record to WAL: {}", entry);
			WalEntry walEntry = WalEntry.newBuilder().setFileOperationRequest(Any.pack(entry)).build();
			return kafkaTemplate.send("mdfs.wal", walEntry).get(10, TimeUnit.SECONDS).getRecordMetadata().timestamp();
		} catch (ExecutionException | TimeoutException | InterruptedException e) {
			throw new Exception(e);
		}
	}

	@Override
	public void close() throws Exception {

	}

}
