package io.github.belugabehr.mdfs.client.core.op;

import java.nio.file.Path;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;
import io.github.belugabehr.mdfs.client.op.ListFilesBuilder;

public class MdfsListFilesBuilder implements ListFilesBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(MdfsGetDataBuilder.class);

	private final FileOperationClient fileOpClient;
	private Path target;
	private boolean longList;

	public MdfsListFilesBuilder(FileOperationClient fileOpClient) {
		this.fileOpClient = fileOpClient;
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
		throw new UnsupportedOperationException();
//		String namespace = MdfsPaths.parseNamespace(this.target);

//		ListFileResponse response = this.fileOpClient
//				.request(ListFileRequest.newBuilder().setLongListing(true)
//						.setListById(ListByFileId.newBuilder().addFileId(MFile.MFileId.newBuilder()
//								.setNamespace(namespace).setId(ByteString.copyFromUtf8(fileName))))
//						.build())

//		return response.getFileList();
	}
}
