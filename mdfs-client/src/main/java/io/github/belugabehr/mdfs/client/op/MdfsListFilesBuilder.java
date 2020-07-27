package io.github.belugabehr.mdfs.client.op;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileResponse;
import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.client.MdfsPaths;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;

public class MdfsListFilesBuilder implements ListFilesBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(MdfsGetDataBuilder.class);

	private final FileOperationClient fileOpClient;
	private Path target;
	private boolean longList;

	public MdfsListFilesBuilder(FileOperationClient fileOpClient) {
		this.fileOpClient = Objects.requireNonNull(fileOpClient);
		this.longList = false;
	}

	public MdfsListFilesBuilder fromNamespace(Path path) {
		this.target = path;
		return this;
	}

	public MdfsListFilesBuilder asLongListing(boolean longList) {
		this.longList = longList;
		return this;
	}

	public Collection<MFile> list() {
		String namespace = MdfsPaths.parseNamespace(this.target);

		ListFileResponse response = this.fileOpClient.request(ListFileRequest.newBuilder()
				.setListByNamespace(ListFileRequest.ListByNamespace.newBuilder().setNamespace(namespace)).build());

		return response.getFileList();
	}
}
