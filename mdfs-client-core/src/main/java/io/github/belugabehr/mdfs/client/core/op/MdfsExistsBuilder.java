package io.github.belugabehr.mdfs.client.core.op;

import java.nio.file.Path;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest.ListByFileId;
import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.client.Stats;
import io.github.belugabehr.mdfs.client.core.MdfsStats;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;
import io.github.belugabehr.mdfs.client.op.ExistsBuilder;

public class MdfsExistsBuilder implements ExistsBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsExistsBuilder.class);

	private final FileOperationClient fileOpClient;

	public MdfsExistsBuilder(FileOperationClient fileOpClient) {
		this.fileOpClient = Objects.requireNonNull(fileOpClient);
	}

	@Override
	public Stats forPath(Path path) {
		String[] elements = path.toUri().getPath().split("/");
		String fileName = elements[elements.length - 1];
		String namespace = elements[elements.length - 2];

		LOG.info("forPath {}", path.toUri().getPath());

		MFile file = Iterables.getOnlyElement(this.fileOpClient
				.request(ListFileRequest.newBuilder().setLongListing(false)
						.setListByFileId(ListByFileId.newBuilder().addFileId(MFile.MFileId.newBuilder()
								.setNamespace(namespace).setId(ByteString.copyFromUtf8(fileName))))
						.build())
				.getFileList());

		LOG.warn("Stat MFile: {}", file);

		return new MdfsStats(file);
	}

}
