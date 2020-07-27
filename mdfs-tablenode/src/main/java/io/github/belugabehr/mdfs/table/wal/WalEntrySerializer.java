package io.github.belugabehr.mdfs.table.wal;

import org.apache.kafka.common.serialization.Serializer;

import com.google.protobuf.MessageLite;

public class WalEntrySerializer implements Serializer<MessageLite> {

	@Override
	public byte[] serialize(String topic, MessageLite msg) {
		return msg.toByteArray();
	}

}
