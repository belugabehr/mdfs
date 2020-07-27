package io.github.belugabehr.mdfs.table.file;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.github.belugabehr.mdfs.api.FileOperations.FileOperationResponse;

public class FileOperationResponseFuture implements Future<FileOperationResponse> {

	private final Lock lock = new ReentrantLock();
	private final Condition hasResult = lock.newCondition();
	private volatile FileOperationResponse response = null;

	@Override
	public FileOperationResponse get() throws InterruptedException, ExecutionException {
		lock.lock();
		try {
			while (this.response == null) {
				this.hasResult.await();
			}
		} finally {
			lock.unlock();
		}
		return this.response;
	}

	@Override
	public FileOperationResponse get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		long nanos = unit.toNanos(timeout);
		lock.lock();
		try {
			while (this.response == null) {
				if (nanos <= 0L) {
					throw new TimeoutException();
				}
				nanos = this.hasResult.awaitNanos(nanos);
			}
		} finally {
			lock.unlock();
		}
		return this.response;
	}

	public void set(FileOperationResponse response) {
		lock.lock();
		try {
			this.response = response;
			this.hasResult.signal();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isDone() {
		return response != null;
	}
}
