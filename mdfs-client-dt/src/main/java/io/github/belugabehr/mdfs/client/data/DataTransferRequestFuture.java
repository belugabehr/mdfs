package io.github.belugabehr.mdfs.client.data;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.github.belugabehr.mdfs.api.DataTransferOperations.DataTransferResponse;

public final class DataTransferRequestFuture implements Future<DataTransferResponse> {

	private final Lock lock = new ReentrantLock(true);
	private final Condition newEntry = lock.newCondition();
	private volatile DataTransferResponse response = null;

	@Override
	public boolean cancel(boolean arg0) {
		return false;
	}

	@Override
	public DataTransferResponse get() throws InterruptedException, ExecutionException {
		this.lock.lock();
		try {
			while (this.response == null) {
				this.newEntry.await();
			}
		} finally {
			this.lock.unlock();
		}
		return this.response;
	}

	@Override
	public DataTransferResponse get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		long nanos = unit.toNanos(timeout);
		lock.lock();
		try {
			while (this.response == null) {
				if (nanos <= 0L) {
					return null;
				}
				nanos = this.newEntry.awaitNanos(nanos);
			}
		} finally {
			lock.unlock();
		}
		return this.response;
	}

	public void setResponse(DataTransferResponse response) {
		this.lock.lock();
		try {
			this.response = response;
			this.newEntry.signalAll();
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return this.response != null;
	}

}
