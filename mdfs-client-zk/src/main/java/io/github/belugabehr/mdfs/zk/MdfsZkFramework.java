package io.github.belugabehr.mdfs.zk;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.apache.curator.framework.CuratorFramework;

import io.github.belugabehr.mdfs.client.MdfsFramework;
import io.github.belugabehr.mdfs.client.core.op.MdfsCreateBuilder;
import io.github.belugabehr.mdfs.client.core.op.MdfsDeleteBuilder;
import io.github.belugabehr.mdfs.client.core.op.MdfsExistsBuilder;
import io.github.belugabehr.mdfs.client.core.op.MdfsGetDataBuilder;
import io.github.belugabehr.mdfs.client.core.op.MdfsListFilesBuilder;
import io.github.belugabehr.mdfs.client.data.DataTransferClient;
import io.github.belugabehr.mdfs.client.data.MdfsDataTransferClient;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;
import io.github.belugabehr.mdfs.client.op.CreateBuilder;
import io.github.belugabehr.mdfs.client.op.DeleteBuilder;
import io.github.belugabehr.mdfs.client.op.ExistsBuilder;
import io.github.belugabehr.mdfs.client.op.GetDataBuilder;
import io.github.belugabehr.mdfs.client.op.ListFilesBuilder;
import io.github.belugabehr.mdfs.zk.file.ZooKeeperFileOperationClient;

public final class MdfsZkFramework implements MdfsFramework {
	private final FileOperationClient fileOpClient;
	private final DataTransferClient dataTxClient;

	private MdfsZkFramework(FileOperationClient fileOpClient, DataTransferClient dataTxClient) {
		this.fileOpClient = fileOpClient;
		this.dataTxClient = dataTxClient;
	}

	@Override
	public void close() throws Exception {
		this.fileOpClient.close();
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public CreateBuilder create() {
		return new MdfsCreateBuilder(fileOpClient, dataTxClient);
	}

	@Override
	public GetDataBuilder getData() {
		return new MdfsGetDataBuilder(fileOpClient, dataTxClient);
	}

	@Override
	public ListFilesBuilder listFiles() {
		return new MdfsListFilesBuilder(fileOpClient);
	}

	@Override
	public ExistsBuilder checkExists() {
		return new MdfsExistsBuilder(fileOpClient);
	}

	@Override
	public DeleteBuilder delete() {
		return new MdfsDeleteBuilder(fileOpClient);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {
		private Optional<CuratorFramework> curator = Optional.empty();
		private Collection<URI> zkHosts = new ArrayList<>();

		public Builder withHosts(Collection<URI> zkUris) {
			zkHosts.addAll(zkUris);
			return this;
		}

		public Builder usingCurator(CuratorFramework curator) {
			this.curator = Optional.ofNullable(curator);
			return this;
		}

		public MdfsZkFramework build() throws InterruptedException, IOException {
			ZooKeeperFileOperationClient fileOpClient = new ZooKeeperFileOperationClient(this.curator.get());
			DataTransferClient dataTxClient = new MdfsDataTransferClient();

			return new MdfsZkFramework(fileOpClient, dataTxClient);
		}
	}

}
