package io.github.belugabehr.mdfs.table.wal;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import io.github.belugabehr.mdfs.api.FileOperations.FileOperationRequest;

public class MemoryWriteAheadLog implements WriteAheadLog<Long, FileOperationRequest> {

	private AtomicLong lastSeq = new AtomicLong(0);
	private BlockingQueue<FileOperationRequest> queue = new LinkedBlockingDeque<>();

	@Override
	public synchronized Long add(FileOperationRequest entry) throws Exception {
		this.queue.add(entry);
		return lastSeq.incrementAndGet();
	}

	@Override
	public void close() throws Exception {

	}

}
