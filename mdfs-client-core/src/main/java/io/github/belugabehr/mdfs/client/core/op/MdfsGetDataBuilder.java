package io.github.belugabehr.mdfs.client.core.op;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.zip.CRC32C;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.DataTransferOperations.ReadBlockRequest;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest.ListByFileId;
import io.github.belugabehr.mdfs.api.Mdfs.MBlock;
import io.github.belugabehr.mdfs.api.Mdfs.MBlockChunk;
import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.client.data.DataTransferClient;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;
import io.github.belugabehr.mdfs.client.op.GetDataBuilder;

public class MdfsGetDataBuilder implements GetDataBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsGetDataBuilder.class);

	private final FileOperationClient fileOpClient;
	private final DataTransferClient dataTxClient;
	private Path target;

	public MdfsGetDataBuilder(FileOperationClient fileOpClient, DataTransferClient dataTxClient) {
		this.fileOpClient = fileOpClient;
		this.dataTxClient = dataTxClient;
	}

	public MdfsGetDataBuilder fromPath(Path path) {
		this.target = path;
		return this;
	}

	@Override
	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
		asInputStream().transferTo(baos);
		return baos.toByteArray();
	}

	@Override
	public ByteBuffer asByteBuffer() throws IOException {
		return ByteBuffer.wrap(getBytes());
	}

	@Override
	public InputStream asInputStream() throws IOException {
		String[] elements = this.target.toUri().getPath().split("/");
		String fileName = elements[elements.length - 1];
		String namespace = elements[elements.length - 2];

		MFile file = Iterables.getOnlyElement(this.fileOpClient
				.request(ListFileRequest.newBuilder().setLongListing(true)
						.setListByFileId(ListByFileId.newBuilder().addFileId(MFile.MFileId.newBuilder()
								.setNamespace(namespace).setId(ByteString.copyFromUtf8(fileName))))
						.build())
				.getFileList());

		LOG.warn("Open MFile: {}", file);

		return new DfsInputStream(this.dataTxClient, file);
	}

	class DfsInputStream extends InputStream {

		private final DataTransferClient dataTxClient;
		private final MFile file;
		private final ByteBuffer buffer;
		private boolean filled;

		public DfsInputStream(DataTransferClient dataTxClient, MFile file) {
			this.dataTxClient = Objects.requireNonNull(dataTxClient);
			this.file = Objects.requireNonNull(file);
			this.buffer = ByteBuffer.allocate(Math.toIntExact(file.getFileSize()));
			this.filled = false;
		}

		@Override
		public int read() throws IOException {
			if (!this.filled) {
				fillBuffer();
				this.filled = true;
			}
			if (this.buffer.hasRemaining()) {
				byte b = this.buffer.get();
				int rtn = b & 0xff;
				return rtn;
			} else {
				return -1;
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (!this.filled) {
				fillBuffer();
				this.filled = true;
			}
			if (this.buffer.hasRemaining()) {
				int n = Math.min(this.buffer.remaining(), len);
				this.buffer.get(b, off, n);
				return n;
			} else {
				return -1;
			}
		}

		private void fillBuffer() throws IOException {
			for (final MBlock block : this.file.getBlockList()) {
				final URI node = URI.create(block.getLocation(0));

				Collection<MBlockChunk> chunks = this.dataTxClient.request(node,
						ReadBlockRequest.newBuilder().setBlockId(block.getBlockDetails().getBlockId()).setChunkStart(0)
								.setChunkStop(Integer.MAX_VALUE).build());
				LOG.warn("Chunks received: {}", chunks.size());
				for (final MBlockChunk chunk : chunks) {
					LOG.warn("Chunk [{}]", chunk);
					chunk.getData().copyTo(this.buffer);
				}
			}
			this.buffer.flip();
			CRC32C crc = new CRC32C();
			crc.update(this.buffer.asReadOnlyBuffer());
			LOG.warn("Got Data [crc32c:{}]", crc.getValue());
		}
	}

}
