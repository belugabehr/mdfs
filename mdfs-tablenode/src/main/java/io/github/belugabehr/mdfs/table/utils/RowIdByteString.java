package io.github.belugabehr.mdfs.table.utils;

import com.google.protobuf.ByteString;

/**
 * Wrapper around {@link ByteString} to make it naturally comparable with
 * unsigned Lexicographical Comparator.
 */
public final class RowIdByteString implements Comparable<RowIdByteString> {

	private final ByteString byteString;

	private RowIdByteString(final ByteString byteString) {
		this.byteString = byteString;
	}

	@Override
	public int compareTo(final RowIdByteString other) {
		return ByteString.unsignedLexicographicalComparator().compare(this.byteString, other.byteString);
	}

	public static RowIdByteString wrap(final ByteString byteString) {
		return new RowIdByteString(byteString);
	}

	@Override
	public String toString() {
		return "RowIdByteString [byteString=" + byteString + "]";
	}

}
