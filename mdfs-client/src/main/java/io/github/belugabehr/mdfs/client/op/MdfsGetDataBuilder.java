package io.github.belugabehr.mdfs.client.op;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.DataTransferOperations.ReadBlockRequest;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest;
import io.github.belugabehr.mdfs.api.Mdfs.MBlock;
import io.github.belugabehr.mdfs.api.Mdfs.MBlockChunk;
import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.api.Mdfs.MFile.MFileId;
import io.github.belugabehr.mdfs.client.MdfsPaths;
import io.github.belugabehr.mdfs.client.data.DataTransferClient;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;

public class MdfsGetDataBuilder implements GetDataBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsGetDataBuilder.class);

	private final FileOperationClient fileOpClient;
	private DataTransferClient dataTxClient;
	private Path target;

	public MdfsGetDataBuilder(FileOperationClient fileOpClient, DataTransferClient dataTxClient) {
		this.fileOpClient = Objects.requireNonNull(fileOpClient);
		this.dataTxClient = Objects.requireNonNull(dataTxClient);
	}

	public MdfsGetDataBuilder fromPath(Path path) {
		this.target = path;
		return this;
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
		asInputStream().transferTo(baos);
		return baos.toByteArray();
	}

	public ByteBuffer asByteBuffer() throws IOException {
		return ByteBuffer.wrap(getBytes());
	}

	public InputStream asInputStream() throws IOException {
		Map.Entry<String, String> namespaceAndId = MdfsPaths.parse(this.target);

		MFile file = Iterables.getOnlyElement(this.fileOpClient
				.request(ListFileRequest.newBuilder().setLongListing(true)
						.setListByFileId(ListFileRequest.ListByFileId.newBuilder()
								.addFileId(MFileId.newBuilder().setNamespace(namespaceAndId.getKey())
										.setId(ByteString.copyFromUtf8(namespaceAndId.getValue()))))
						.build())
				.getFileList());

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
				URI node = URI.create(block.getLocationList().get(0));
				Collection<MBlockChunk> chunks = this.dataTxClient
						.request(node, ReadBlockRequest.newBuilder().setBlockId(block.getBlockDetails().getBlockId())
								.setChunkStart(0).setChunkStop(Integer.MAX_VALUE).build());
				for (final MBlockChunk chunk : chunks) {
					LOG.warn("Chunk [{}]", chunk);
					chunk.getData().copyTo(this.buffer);
				}
//				}
			}
			this.buffer.flip();
		}
	}

}
