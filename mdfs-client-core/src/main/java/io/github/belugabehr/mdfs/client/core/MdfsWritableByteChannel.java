package io.github.belugabehr.mdfs.client.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.github.belugabehr.mdfs.api.Mdfs.Replication;
import io.github.belugabehr.mdfs.client.MdfsFramework;

public class MdfsWritableByteChannel implements SeekableByteChannel {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsWritableByteChannel.class);

	private final MdfsFramework client;
	private final Path path;

	private boolean open;

	private final Path tmpPath;
	private final SeekableByteChannel tmpPathChannel;

	public MdfsWritableByteChannel(MdfsFramework client, Path path, Set<? extends OpenOption> options)
			throws IOException {
		this.client = Objects.requireNonNull(client);
		this.path = Objects.requireNonNull(path);

		Objects.requireNonNull(options);

		Preconditions.checkArgument(options.contains(StandardOpenOption.WRITE));
		Preconditions.checkArgument(!options.contains(StandardOpenOption.READ));
		this.open = true;

		this.tmpPath = Files.createTempFile("mdfs_", ".tmp");
		this.tmpPathChannel = Files.newByteChannel(this.tmpPath, StandardOpenOption.WRITE);
	}

	@Override
	public long position() throws IOException {
		return this.tmpPathChannel.position();
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		checkOpen();

		this.tmpPathChannel.position(newPosition);
		return this;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long size() throws IOException {
		checkOpen();

		return this.tmpPathChannel.size();
	}

	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		checkOpen();

		this.tmpPathChannel.truncate(size);
		return this;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		checkOpen();

		return this.tmpPathChannel.write(src);
	}

	void checkOpen() throws ClosedChannelException {
		if (!this.isOpen()) {
			throw new ClosedChannelException();
		}
	}

	@Override
	public void close() throws IOException {
		this.open = false;
		this.tmpPathChannel.close();
		LOG.warn("File size: {}", Files.size(this.tmpPath));
		Replication repl = Replication.newBuilder()
				.setFan(Replication.Fan.newBuilder().setMinReplicas(1).setReplicas(1)).build();
		this.client.create().withPath(this.path).withReplication(repl).copyFromFile(this.tmpPath);
	}

	@Override
	public boolean isOpen() {
		return this.open;
	}

	@Override
	public String toString() {
		return "MdfsByteChannel [path=" + path + ", open=" + open + ", tmpPath=" + tmpPath + "]";
	}

}
