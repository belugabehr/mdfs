package io.github.belugabehr.mdfs.table.wal;

public interface WriteAheadLog<S, E> extends AutoCloseable {
	S add(final E entry) throws Exception;
}
