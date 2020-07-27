package io.github.belugabehr.mdfs.table.wal;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.protobuf.InvalidProtocolBufferException;

import io.github.belugabehr.mdfs.tablenode.TableNode.WalEntry;

public class WalEntryDeserializer implements Deserializer<WalEntry> {

	@Override
	public WalEntry deserialize(String topic, byte[] data) {
		try {
			return WalEntry.parseFrom(data);
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}
	}

}
