package io.github.belugabehr.mdfs.client.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.github.belugabehr.mdfs.client.MdfsFramework;

public class MdfsReadableByteChannel implements SeekableByteChannel {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsReadableByteChannel.class);

	private final MdfsFramework client;
	private final Path path;

	private boolean open;

	private final ByteBuffer buffer;

	public MdfsReadableByteChannel(MdfsFramework client, Path path, Set<? extends OpenOption> options)
			throws IOException {
		this.client = Objects.requireNonNull(client);
		this.path = Objects.requireNonNull(path);

		Objects.requireNonNull(options);

		Preconditions.checkArgument(options.contains(StandardOpenOption.READ));
		Preconditions.checkArgument(!options.contains(StandardOpenOption.WRITE));

		this.buffer = client.getData().fromPath(path).asByteBuffer();
		LOG.error("BUFFER [{}]", this.buffer);

		this.open = true;
	}

	@Override
	public long position() throws IOException {
		return this.buffer.position();
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		this.buffer.position(Math.toIntExact(newPosition));
		return this;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		Objects.requireNonNull(dst);
		checkOpen();

//		LOG.error("Read: {}::{}", this.buffer.position(), this.buffer.remaining());

		if (this.buffer.remaining() == 0) {
			return -1;
		}

		int nTransfer = Math.min(this.buffer.remaining(), dst.remaining());
		for (int i = 0; i < nTransfer; i++) {
			dst.put(this.buffer.get());
		}
		return nTransfer;
	}

	@Override
	public long size() throws IOException {
		checkOpen();

		return 0L;
	}

	@Override
	public SeekableByteChannel truncate(long arg0) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		throw new UnsupportedOperationException();
	}

	void checkOpen() throws ClosedChannelException {
		if (!this.isOpen()) {
			throw new ClosedChannelException();
		}
	}

	@Override
	public void close() throws IOException {
		this.open = false;
	}

	@Override
	public boolean isOpen() {
		return this.open;
	}

}
