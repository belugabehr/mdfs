package io.github.belugabehr.mdfs.datanode.block;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.protobuf.Any;

import io.github.belugabehr.mdfs.api.DataTransferOperations.DeleteBlockRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.ReadBlockRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.WriteBlockRequest;
import io.github.belugabehr.mdfs.api.Mdfs.MBlock;
import io.github.belugabehr.mdfs.api.Mdfs.MBlockDetails;
import io.github.belugabehr.mdfs.datanode.DataNode.LocalMBlock;
import io.github.belugabehr.mdfs.datanode.util.BlockFileIterator;

@Service
public class BlockService implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(BlockService.class);

	private static final ReadOptions SKIP_CACHE = new ReadOptions().fillCache(false);

	private static final ReadOptions DEFAULT = new ReadOptions();

	@Value("${datanode.meta.home:/var/lib/springdn}")
	private String dataDir;

	@Value("${datanode.meta.cache:33554432}")
	private long cacheSize;

	private DB db;

	@PostConstruct
	public void init() throws IOException {
		final Path metaDir = Paths.get(dataDir, "meta");
		Files.createDirectories(metaDir);
		this.db = JniDBFactory.factory.open(metaDir.toFile(), new Options().cacheSize(cacheSize).blockSize(1024 * 32)
				.paranoidChecks(true).compressionType(CompressionType.SNAPPY));
	}

	@PreDestroy
	@Override
	public void close() throws IOException {
		LOG.info("Shutting down Block Metadata Store");
		this.db.close();
	}

	public LocalMBlock writeBlock(final WriteBlockRequest request) throws IOException {
		Objects.requireNonNull(request);

		LOG.debug("Writing block: [{}]", request);

		final MBlock block = request.getBlock();
		final MBlockDetails blockDetails = block.getBlockDetails();

		final byte[] blockIdBytes = blockDetails.getBlockId().toByteArray();

		final Path filePath = Files.createTempFile("my-file", ".blockfile");

		// Wrap BlockDetails in a checksum
		try (OutputStream fos = new BufferedOutputStream(new FileOutputStream(filePath.toFile()))) {
			blockDetails.writeDelimitedTo(fos);
		}

		final LocalMBlock localBlock = LocalMBlock.newBuilder().setLocation(filePath.toUri().toString())
				.setBlockDetails(Any.pack(blockDetails)).build();

		LOG.warn("Local Block stored: {}", localBlock);
		LOG.warn("Block details stored: {}", blockDetails);

		final byte[] blockBytes = localBlock.toByteArray();

		try {
			final WriteBatch updates = this.db.createWriteBatch().put(blockIdBytes, blockBytes);
			this.db.write(updates);
		} catch (DBException e) {
			throw new IOException(e);
		}

		return localBlock;
	}

	public BlockFileIterator readBlock(ReadBlockRequest request) throws IOException {
		Objects.requireNonNull(request);

		LOG.warn("Reading block: [{}]", request);

		byte[] value = this.db.get(request.getBlockId().toByteArray(), DEFAULT);
		LocalMBlock localBlock = LocalMBlock.parseFrom(value);
		MBlockDetails details = localBlock.getBlockDetails().unpack(MBlockDetails.class);

		LOG.warn("Local block: [{}]", localBlock);

		Path path = Paths.get(URI.create(localBlock.getLocation()));

		return new BlockFileIterator(Optional.of(details), path);
	}

	public void deleteBlock(DeleteBlockRequest request) throws IOException {
		Objects.requireNonNull(request);

		LOG.warn("Deleting block: [{}]", request);

		final byte[] blockBytes = request.getBlockId().toByteArray();

		try {
			this.db.delete(blockBytes);
		} catch (DBException e) {
			throw new IOException(e);
		}

	}
}
