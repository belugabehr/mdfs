package io.github.belugabehr.mdfs.table.utils;

public final class CachingInputFileKey {

	private final long position;
	private final int size;

	private CachingInputFileKey(long position, int size) {
		this.position = position;
		this.size = size;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (position ^ (position >>> 32));
		result = prime * result + size;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		CachingInputFileKey other = (CachingInputFileKey) obj;
		if (position != other.position) {
			return false;
		}
		if (size != other.size) {
			return false;
		}
		return true;
	}

	static CachingInputFileKey ofPair(long position, int size) {
		return new CachingInputFileKey(position, size);
	}
}
