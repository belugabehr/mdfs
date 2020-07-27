package io.github.belugabehr.mdfs.zk.file;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.belugabehr.mdfs.api.FileOperations.CreateFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.CreateFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.DeleteFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.DeleteFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest.ListByFileId;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest.ListByNamespace;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileResponse;
import io.github.belugabehr.mdfs.api.Mdfs.MFile;
import io.github.belugabehr.mdfs.api.Mdfs.MFile.MFileId;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;

public class ZooKeeperFileOperationClient implements FileOperationClient {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperFileOperationClient.class);

	private final CuratorFramework client;

	public ZooKeeperFileOperationClient(CuratorFramework client) {
		this.client = client;
	}

	public CreateFileResponse request(CreateFileRequest request) {
		LOG.info("CreateFileRequest: {}", request);

		Objects.requireNonNull(request);

		MFile file = request.getFile();
		byte[] data = file.toByteArray();
		String path = ZKPaths.makePath(file.getFileId().getNamespace(), file.getFileId().getId().toStringUtf8());
		try {
			LOG.info("Creating path {} in namespace {}", path, client.getNamespace());
			this.client.create().creatingParentsIfNeeded().forPath(path, data);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return CreateFileResponse.newBuilder().setFile(file).addLocation("tcp://localhost:51515").build();
	}

	public FinalizeFileResponse request(FinalizeFileRequest request) {
		LOG.info("FinalizeFileRequest: {}", request);
		MFile file = Objects.requireNonNull(request).getFile();

		byte[] data = file.toByteArray();
		String path = ZKPaths.makePath(file.getFileId().getNamespace(), file.getFileId().getId().toStringUtf8());
		try {
			this.client.setData().forPath(path, data);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return FinalizeFileResponse.newBuilder().setFile(file).build();
	}

	public ListFileResponse request(ListFileRequest request) {
		LOG.info("ListFileRequest: {}", request);
		Objects.requireNonNull(request);

		switch (request.getListTypeCase()) {
		case LIST_BY_FILE_ID:
			return doListById(request);
		case LIST_BY_NAMESPACE:
			return doListByNamespace(request);
		case LISTTYPE_NOT_SET:
		default:
			throw new RuntimeException("No list type specified");
		}
	}

	private ListFileResponse doListById(ListFileRequest request) {
		ListByFileId listByFileId = request.getListByFileId();
		Preconditions.checkArgument(1 == listByFileId.getFileIdCount(), "Only listing one file at a time is supported");
		MFile.MFileId fileId = listByFileId.getFileId(0);
		try {
			String childPath = ZKPaths.makePath(fileId.getNamespace(), fileId.getId().toStringUtf8());
			byte[] data = this.client.getData().forPath(childPath);
			return ListFileResponse.newBuilder().addFile(MFile.parseFrom(data)).build();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private ListFileResponse doListByNamespace(ListFileRequest request) {
		ListByNamespace listByNamespace = request.getListByNamespace();
		try {
			ListFileResponse.Builder builder = ListFileResponse.newBuilder();
			String namespacePath = listByNamespace.getNamespace();
			List<String> children = this.client.getChildren().forPath(namespacePath);
			for (String child : children) {
				String childPath = ZKPaths.makePath(namespacePath, child);
				byte[] data = this.client.getData().forPath(childPath);
				builder.addFile(MFile.parseFrom(data));
			}
			return builder.build();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public DeleteFileResponse request(DeleteFileRequest request) {
		LOG.info("DeleteFileRequest: {}", request);
		Objects.requireNonNull(request);
		MFileId fileId = request.getFileId();

		String path = ZKPaths.makePath(fileId.getNamespace(), fileId.getId().toStringUtf8());
		try {
			// TODO: Do not actually delete it, just mark it as deleted so that blocks can be cleaned up later
			this.client.delete().guaranteed().forPath(path);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	@Override
	public void close() throws IOException {

	}
}
