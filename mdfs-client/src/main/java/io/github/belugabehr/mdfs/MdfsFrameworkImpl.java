package io.github.belugabehr.mdfs;

import java.io.IOException;

import io.github.belugabehr.mdfs.client.MdfsFramework;
import io.github.belugabehr.mdfs.client.data.DataTransferClient;
import io.github.belugabehr.mdfs.client.data.MdfsDataTransferClient;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;
import io.github.belugabehr.mdfs.client.file.MdfsFileOperationClient;
import io.github.belugabehr.mdfs.client.op.CreateBuilder;
import io.github.belugabehr.mdfs.client.op.DeleteBuilder;
import io.github.belugabehr.mdfs.client.op.ExistsBuilder;
import io.github.belugabehr.mdfs.client.op.GetDataBuilder;
import io.github.belugabehr.mdfs.client.op.ListFilesBuilder;
import io.github.belugabehr.mdfs.client.op.MdfsCreateBuilder;
import io.github.belugabehr.mdfs.client.op.MdfsGetDataBuilder;
import io.github.belugabehr.mdfs.client.op.MdfsListFilesBuilder;

public final class MdfsFrameworkImpl implements MdfsFramework {

	private final FileOperationClient fileOpClient;
	private final DataTransferClient dataTxClient;

	private MdfsFrameworkImpl(FileOperationClient fileOpClient, DataTransferClient dataTxClient) {
		this.fileOpClient = fileOpClient;
		this.dataTxClient = dataTxClient;
	}

	@Override
	public void close() throws Exception {
		this.fileOpClient.close();
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

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {
		private String host;
		private int port;

		public Builder withHost(String host) {
			this.host = host;
			return this;
		}

		public Builder withPort(int port) {
			this.port = port;
			return this;
		}

		public MdfsFrameworkImpl build() throws InterruptedException, IOException {
			FileOperationClient fileOpClient = MdfsFileOperationClient.newBuilder().withHost(this.host)
					.withPort(this.port).build();
			DataTransferClient dataTxClient = new MdfsDataTransferClient();
			return new MdfsFrameworkImpl(fileOpClient, dataTxClient);
		}
	}

	public boolean isOpen() {
		return true;
	}

	@Override
	public ExistsBuilder checkExists() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DeleteBuilder delete() {
		// TODO Auto-generated method stub
		return null;
	}

}
