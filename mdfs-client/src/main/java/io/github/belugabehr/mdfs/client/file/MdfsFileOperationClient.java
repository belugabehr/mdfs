package io.github.belugabehr.mdfs.client.file;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.FileOperations.CreateFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.CreateFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.DeleteFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.DeleteFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.FileOperationErrorResponse;
import io.github.belugabehr.mdfs.api.FileOperations.FileOperationRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FileOperationResponse;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FinalizeFileResponse;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileRequest;
import io.github.belugabehr.mdfs.api.FileOperations.ListFileResponse;
import io.github.belugabehr.mdfs.client.file.FileOperationClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class MdfsFileOperationClient implements FileOperationClient {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsFileOperationClient.class);

	private final EventLoopGroup workerGroup = new NioEventLoopGroup();
	private String host;
	private int port;

	private ChannelFuture channelFuture;
	private MdfsFileOperationClientHandler clientHandler;

	private MdfsFileOperationClient() {
	}

	public CreateFileResponse request(CreateFileRequest request) {
		LOG.info("CreateFileRequest: " + request);
		FileOperationRequest wrapper = FileOperationRequest.newBuilder()
				.setId(ByteString.copyFromUtf8(UUID.randomUUID().toString())).setCreateFileRequest(request).build();
		clientHandler.sendRequest(wrapper);
		FileOperationResponse response = clientHandler.getResponse();

		checkException(response);

		return response.getCreateFileResponse();
	}

	public FinalizeFileResponse request(FinalizeFileRequest request) {
		LOG.info("FinalizeFileRequest: " + request);
		FileOperationRequest wrapper = FileOperationRequest.newBuilder()
				.setId(ByteString.copyFromUtf8(UUID.randomUUID().toString())).setFinalizeFileRequest(request).build();
		clientHandler.sendRequest(wrapper);
		FileOperationResponse response = clientHandler.getResponse();

		checkException(response);

		return response.getFinalizeFileResponse();
	}

	public ListFileResponse request(ListFileRequest request) {
		LOG.info("ListNamespaceResponse: " + request);
		FileOperationRequest wrapper = FileOperationRequest.newBuilder()
				.setId(ByteString.copyFromUtf8(UUID.randomUUID().toString())).setListFileRequest(request).build();
		clientHandler.sendRequest(wrapper);
		FileOperationResponse response = clientHandler.getResponse();

		checkException(response);

		return response.getListFileResponse();
	}

	public DeleteFileResponse request(DeleteFileRequest request) {
		LOG.info("DeleteFileRequest: " + request);
		FileOperationRequest wrapper = FileOperationRequest.newBuilder()
				.setId(ByteString.copyFromUtf8(UUID.randomUUID().toString())).setDeleteFileRequest(request).build();
		clientHandler.sendRequest(wrapper);
		FileOperationResponse response = clientHandler.getResponse();

		checkException(response);

		return response.getDeleteFileResponse();
	}

	private void checkException(FileOperationResponse response) {
		if (response.getRequestCase() == FileOperationResponse.RequestCase.ERRORRESPONSE) {
			FileOperationErrorResponse errorResponse = response.getErrorResponse();
			switch (errorResponse.getRequestCase()) {
			case CREATEFILEREQUEST:
				throw new RuntimeException(errorResponse.getErrorMessage());
			case REQUEST_NOT_SET:
				break;
			default:
				break;
			}
		}
	}

	public void connect() throws InterruptedException {
		Bootstrap b = new Bootstrap();
		b.group(workerGroup);
		b.channel(NioSocketChannel.class);
		b.option(ChannelOption.SO_KEEPALIVE, true);
		b.handler(new MdfsFileOperationClientInitializer());

		// Start the client.
		this.channelFuture = b.connect(this.host, this.port).sync();

		this.clientHandler = channelFuture.channel().pipeline().get(MdfsFileOperationClientHandler.class);
	}

	@Override
	public void close() throws IOException {
		try {
			channelFuture.channel().close().sync();
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new IOException("Failed to close client", ie);
		} finally {
			workerGroup.shutdownGracefully();
		}
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

		public MdfsFileOperationClient build() throws InterruptedException {
			MdfsFileOperationClient client = new MdfsFileOperationClient();
			client.host = this.host;
			client.port = this.port;
			client.connect();
			return client;
		}
	}
}
