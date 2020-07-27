package io.github.belugabehr.mdfs.table.utils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

class CachingSeekableInputStream extends SeekableInputStream {
	private static final Logger LOG = LoggerFactory.getLogger(CachingSeekableInputStream.class);

	private final ByteBuffer oneByteBuffer = ByteBuffer.allocate(1);
	private final SeekableByteChannel channel;
	private final Cache<CachingInputFileKey, ByteBuffer> cache;

	CachingSeekableInputStream(Path path, Cache<CachingInputFileKey, ByteBuffer> cache) throws IOException {
		this.channel = Files.newByteChannel(path, StandardOpenOption.READ);
		this.cache = cache;
	}

	@Override
	public long getPos() throws IOException {
		return this.channel.position();
	}

	@Override
	public void seek(long newPos) throws IOException {
		this.channel.position(newPos);
	}

	@Override
	public void readFully(byte[] bytes) throws IOException {
		LOG.info("fullyRead(byte[] bytes)");
		CachingInputFileKey key = CachingInputFileKey.ofPair(getPos(), bytes.length);
		ByteBuffer bb = null;
		try {
			bb = cache.get(key, new Callable<ByteBuffer>() {
				@Override
				public ByteBuffer call() throws IOException {
					ByteBuffer bb = ByteBuffer.allocate(bytes.length);
					while (bb.hasRemaining()) {
						int read = CachingSeekableInputStream.this.channel.read(bb);
						if (read == -1) {
							throw new EOFException();
						}
					}
					return bb.flip().asReadOnlyBuffer();
				}
			});
		} catch (ExecutionException e) {
			throw new IOException(e.getCause());
		}
		LOG.info("Cache stats: {}", cache.stats());
		bb.asReadOnlyBuffer().get(bytes);
	}

	@Override
	public void readFully(byte[] bytes, int start, int len) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(len);
		this.readFully(bb);
		bb.flip().get(bytes, start, len);
	}

	@Override
	public int read(ByteBuffer buf) throws IOException {
		return this.channel.read(buf);
	}

	@Override
	public void readFully(ByteBuffer buf) throws IOException {
		LOG.info("fullyRead(ByteBuffer buf) Remaining: {}", buf.remaining());
		CachingInputFileKey key = CachingInputFileKey.ofPair(getPos(), buf.remaining());
		ByteBuffer bb = null;
		try {
			bb = cache.get(key, new Callable<ByteBuffer>() {
				@Override
				public ByteBuffer call() throws IOException {
					ByteBuffer chunk = ByteBuffer.allocate(buf.remaining());
					while (chunk.hasRemaining()) {
						int read = CachingSeekableInputStream.this.channel.read(chunk);
						if (read == -1) {
							throw new EOFException();
						}
					}
					return chunk.flip().asReadOnlyBuffer();
				}
			});
		} catch (ExecutionException e) {
			throw new IOException(e.getCause());
		}
		LOG.info("fullyRead(ByteBuffer buf) Result: {}", bb.asReadOnlyBuffer());
		buf.put(bb.asReadOnlyBuffer());
	}

	@Override
	public int read() throws IOException {
		ByteBuffer b = this.oneByteBuffer.duplicate();
		int read = this.channel.read(b);
		if (read < 0) {
			return read;
		}
		return (0xFF & b.flip().get());
	}
}
