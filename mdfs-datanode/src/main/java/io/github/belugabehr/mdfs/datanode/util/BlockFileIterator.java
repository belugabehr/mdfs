package io.github.belugabehr.mdfs.datanode.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.zip.CRC32C;

import com.google.common.io.ByteStreams;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import io.github.belugabehr.mdfs.api.Mdfs.MBlockChunk;
import io.github.belugabehr.mdfs.api.Mdfs.MBlockDetails;

public class BlockFileIterator implements Iterator<MBlockChunk>, AutoCloseable {

	private final MBlockDetails blockDetails;
	private final InputStream is;
	private final CRC32C crc = new CRC32C();
	private final boolean validatePayload = true;
	private final int chunkCount;
	private int currentChunkIndex = 0;

	public BlockFileIterator(Optional<MBlockDetails> details, Path file) throws IOException {
		this.is = new BufferedInputStream(Files.newInputStream(file));

		if (details.isPresent()) {
			this.blockDetails = details.get();
			int skipSize = CodedOutputStream.computeUInt32SizeNoTag(this.blockDetails.getSerializedSize())
					+ this.blockDetails.getSerializedSize();
			ByteStreams.skipFully(this.is, skipSize);
		} else {
			this.blockDetails = MBlockDetails.parseDelimitedFrom(this.is);
		}

		this.chunkCount = (int) (this.blockDetails.getBlockSize() + this.blockDetails.getChunkSize() - 1L)
				/ this.blockDetails.getChunkSize();
	}

	public void skip(int c) throws IOException {
		for (int i = 0; i < c; i++) {
			int firstByte = this.is.read();
			if (firstByte == -1) {
				throw new IOException("Reached end of file before skipping to the record");
			}
			int protoSize = CodedInputStream.readRawVarint32(firstByte, this.is);
			ByteStreams.skipFully(this.is, protoSize);
			currentChunkIndex++;
		}
	}

	@Override
	public boolean hasNext() {
		return this.currentChunkIndex < this.chunkCount;
	}

	@Override
	public MBlockChunk next() {
		MBlockChunk blockChunk;
		try {
			blockChunk = MBlockChunk.parseDelimitedFrom(this.is);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (this.validatePayload) {
			this.crc.update(blockChunk.getData().toByteArray());
			if (blockChunk.getChecksum() != this.crc.getValue()) {
				throw new RuntimeException("CRC mismatch");
			}
			this.crc.reset();
		}
		this.currentChunkIndex++;
		return blockChunk;
	}

	@Override
	public void close() throws IOException {
		this.is.close();
	}

}
