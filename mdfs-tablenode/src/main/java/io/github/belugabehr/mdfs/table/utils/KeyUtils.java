package io.github.belugabehr.mdfs.table.utils;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.google.protobuf.ByteString;

public final class KeyUtils {

	private KeyUtils() {

	}

	public static ByteBuffer ceilKey(final ByteBuffer key) {
		byte[] ceilKey = new byte[key.remaining()];
		key.get(ceilKey);
		int carry = 0;
		for (int i = ceilKey.length - 1; i >= 0; i--) {
			if (++ceilKey[i] == 0) {
				carry++;
			} else {
				break;
			}
		}
		if (carry == ceilKey.length) {
			throw new RuntimeException("Bad luck");
		}
		return ByteBuffer.wrap(ceilKey);

	}

	public static Map.Entry<ByteString, ByteString> getRowPrefix(final ByteString namespace) {
		Objects.requireNonNull(namespace);

		final MessageDigest rowKeyDigester = messageDigest();
		rowKeyDigester.update(namespace.asReadOnlyByteBuffer());

		final ByteBuffer namespaceID = ByteBuffer.wrap(rowKeyDigester.digest());
		final ByteBuffer namespaceIDinc = ceilKey(namespaceID);

		final ByteBuffer startKey = ByteBuffer.allocate(2 * rowKeyDigester.getDigestLength());
		final ByteBuffer stopKey = ByteBuffer.allocate(2 * rowKeyDigester.getDigestLength());

		final int zeros = rowKeyDigester.getDigestLength();
		startKey.put(namespaceID).put(new byte[zeros]).flip();
		stopKey.put(namespaceIDinc).put(new byte[zeros]).flip();

		return new AbstractMap.SimpleEntry<>(ByteString.copyFrom(startKey), ByteString.copyFrom(stopKey));
	}

	public static ByteString getRowKey(final ByteString namespace, final Optional<ByteString> id) {
		Objects.requireNonNull(namespace);
		Objects.requireNonNull(id);

		final MessageDigest rowKeyDigester = messageDigest();

		final ByteBuffer rowKey = ByteBuffer.allocate(2 * rowKeyDigester.getDigestLength());

		rowKeyDigester.update(namespace.asReadOnlyByteBuffer());

		rowKey.put(rowKeyDigester.digest());

		if (id.isPresent()) {
			rowKeyDigester.reset();
			rowKeyDigester.update(id.get().asReadOnlyByteBuffer());
			rowKey.put(rowKeyDigester.digest());
		} else {
			for (int i = 0; i < rowKeyDigester.getDigestLength(); i++) {
				rowKey.put((byte) 0);
			}
		}

		return ByteString.copyFrom(rowKey.flip());
	}

	private static MessageDigest messageDigest() {
		try {
			return MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// MD5 is a requirement for all JRE
			throw new RuntimeException(e);
		}
	}

}
