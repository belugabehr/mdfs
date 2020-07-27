package io.github.belugabehr.mdfs.client.core.op;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.DataTransferOperations.WriteBlockChunksRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.WriteBlockRequest;
import io.github.belugabehr.mdfs.api.FileOperations.CreateFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.CreateFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileResponse;
import io.github.belugabehr.mdfs.api.Mdfs.MBlock;
import io.github.belugabehr.mdfs.api.Mdfs.MBlockChunk;
import io.github.belugabehr.mdfs.api.Mdfs.MBlockDetails;
import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.api.Mdfs.MFile.MFileId;
import io.github.belugabehr.mdfs.api.Mdfs.Replication;
import io.github.belugabehr.mdfs.client.data.DataTransferClient;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;
import io.github.belugabehr.mdfs.client.op.CreateBuilder;

public class MdfsCreateBuilder implements CreateBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsCreateBuilder.class);

	private final FileOperationClient fileOpClient;
	private final DataTransferClient dataTxClient;
	private Path target;
	private Replication replication = Replication.getDefaultInstance();

	public MdfsCreateBuilder(FileOperationClient fileOpClient, DataTransferClient dataTxClient) {
		this.fileOpClient = Objects.requireNonNull(fileOpClient);
		this.dataTxClient = Objects.requireNonNull(dataTxClient);
	}

	@Override
	public MdfsCreateBuilder withPath(Path path) {
		this.target = path;
		return this;
	}

	@Override
	public CreateBuilder withReplication(Replication replication) {
		this.replication = replication;
		return this;
	}

	@Override
	public void copyFromFile(final Path source) throws IOException {
		Objects.requireNonNull(this.target);
		Objects.requireNonNull(source);

		LOG.error("Copying file");

		final long fileSize = Files.size(source);
		final int chunkSize = 8192;
		final long blockSize = fileSize; // chunkSize << 1;

		CRC32 fileCrc = new CRC32();
		fileCrc.update(Files.readAllBytes(source));
		LOG.warn("Write Data [crc32:{}]", fileCrc.getValue());

		String[] elements = target.toUri().getPath().split("/");
		String namespace = elements[elements.length - 2];
		String fileName = elements[elements.length - 1];

		final MFile.Builder mFile = MFile.newBuilder()
				.setFileId(MFileId.newBuilder().setNamespace(namespace).setId(ByteString.copyFromUtf8(fileName)))
				.setFileSize(fileSize).setBlockSize(blockSize).setReplication(this.replication);

		CreateFileResponse cfResponse = this.fileOpClient
				.request(CreateFileRequest.newBuilder().setFile(mFile).build());

		final Collection<String> locations = cfResponse.getLocationList();

		for (String dataNode : locations) {
			final MBlock block = MBlock.newBuilder().addLocation(dataNode)
					.setBlockDetails(
							MBlockDetails.newBuilder().setBlockId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
									.setBlockSize(blockSize).setChunkSize(chunkSize))
					.build();

			URI node = URI.create(dataNode);

			this.dataTxClient.request(node, WriteBlockRequest.newBuilder().setBlock(block).build());

			final CRC32C crc = new CRC32C();
			final ByteBuffer buf = ByteBuffer.allocate(chunkSize);
			final SeekableByteChannel channel = Files.newByteChannel(source);

			long remaining = fileSize;

			do {
				final int sendChunkSize = Math.toIntExact(Math.min(chunkSize, remaining));
				buf.clear().limit(sendChunkSize).rewind();
				while (buf.hasRemaining()) {
					channel.read(buf);
				}
				buf.flip();

				crc.reset();
				crc.update(buf);
				buf.rewind();

				this.dataTxClient.request(node,
						WriteBlockChunksRequest.newBuilder().addChunk(
								MBlockChunk.newBuilder().setChecksum(crc.getValue()).setData(ByteString.copyFrom(buf)))
								.build());

				remaining -= sendChunkSize;
			} while (remaining > 0L);

			channel.close();

			mFile.addAllBlock(Collections.singleton(block));

			FinalizeFileResponse response = this.fileOpClient
					.request(FinalizeFileRequest.newBuilder().setFile(mFile).build());
			LOG.info("Reponse: {}", response);
		}
	}
}
