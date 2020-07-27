package io.github.belugabehr.mdfs.client.core.op;

import java.nio.file.Path;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.FileOperations.DeleteFileRequest;
import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;
import io.github.belugabehr.mdfs.client.op.DeleteBuilder;

public class MdfsDeleteBuilder implements DeleteBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsDeleteBuilder.class);

	private final FileOperationClient fileOpClient;

	public MdfsDeleteBuilder(FileOperationClient fileOpClient) {
		this.fileOpClient = Objects.requireNonNull(fileOpClient);
	}

	@Override
	public void forPath(Path path) {
		String[] elements = path.toUri().getPath().split("/");
		String fileName = elements[elements.length - 1];
		String namespace = elements[elements.length - 2];

		LOG.info("forPath {}", path.toUri().getPath());

		this.fileOpClient.request(DeleteFileRequest.newBuilder()
				.setFileId(MFile.MFileId.newBuilder().setNamespace(namespace).setId(ByteString.copyFromUtf8(fileName)))
				.build());
	}

}
