package io.github.belugabehr.mdfs.table.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import org.apache.parquet.io.PositionOutputStream;

public class ParquetPositionOutputStream extends PositionOutputStream {

	private final ByteBuffer oneByte = ByteBuffer.allocate(1);

	private final SeekableByteChannel channel;

	public ParquetPositionOutputStream(SeekableByteChannel channel) {
		this.channel = channel;
	}

	@Override
	public long getPos() throws IOException {
		return this.channel.position();
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		this.channel.write(ByteBuffer.wrap(b, off, len));
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(int b) throws IOException {
		byte[] ba = new byte[] {(byte) b};
		this.channel.write(ByteBuffer.wrap(ba));
	}

	@Override
	public void close() throws IOException {
		super.close();
		this.channel.close();
	}

}
