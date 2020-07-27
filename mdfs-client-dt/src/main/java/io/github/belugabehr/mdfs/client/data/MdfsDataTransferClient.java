package io.github.belugabehr.mdfs.client.data;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.DataTransferOperations.DataTransferRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.DataTransferResponse;
import io.github.belugabehr.mdfs.api.DataTransferOperations.DeleteBlockRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.ReadBlockRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.ReadBlockResponse;
import io.github.belugabehr.mdfs.api.DataTransferOperations.WriteBlockChunksRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.WriteBlockRequest;
import io.github.belugabehr.mdfs.api.Mdfs.MBlockChunk;
import io.netty.channel.ChannelFuture;

public class MdfsDataTransferClient implements DataTransferClient {

	private static final Logger LOG = LoggerFactory.getLogger(MdfsDataTransferClient.class);

	private final ConnectionFactory connectionFactory;

	public MdfsDataTransferClient() {
		this.connectionFactory = new ConnectionFactory();
	}

	@Override
	public void request(URI node, WriteBlockRequest request) throws IOException {
		LOG.info("WriteBlockRequest [{}], {}", node, request);

		ChannelFuture connection = this.connectionFactory.getConnection(node);
		MdfsDataTransferClientHandler clientHandler = connection.channel().pipeline()
				.get(MdfsDataTransferClientHandler.class);

		DataTransferRequest wrapper = DataTransferRequest.newBuilder()
				.setId(ByteString.copyFromUtf8(UUID.randomUUID().toString())).setWriteBlockRequest(request).build();
		Future<DataTransferResponse> f = clientHandler.sendRequestAsync(wrapper);

		try {
			DataTransferResponse response = f.get();
			LOG.debug("Response: {}", response);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void request(URI node, WriteBlockChunksRequest request) throws IOException {
		LOG.info("WriteBlockChunksRequest {}", request);
		DataTransferRequest wrapper = DataTransferRequest.newBuilder()
				.setId(ByteString.copyFromUtf8(UUID.randomUUID().toString())).setWriteBlockChunksRequest(request)
				.build();

		ChannelFuture connection = this.connectionFactory.getConnection(node);

		MdfsDataTransferClientHandler clientHandler = connection.channel().pipeline()
				.get(MdfsDataTransferClientHandler.class);

		Future<DataTransferResponse> f = clientHandler.sendRequestAsync(wrapper);

		try {
			DataTransferResponse response = f.get();
			LOG.debug("Response: {}", response);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Collection<MBlockChunk> request(URI node, ReadBlockRequest request) throws IOException {
		ChannelFuture connection = this.connectionFactory.getConnection(node);
		MdfsDataTransferClientHandler clientHandler = connection.channel().pipeline()
				.get(MdfsDataTransferClientHandler.class);

		DataTransferRequest wrapper = DataTransferRequest.newBuilder()
				.setId(ByteString.copyFromUtf8((UUID.randomUUID().toString()))).setReadBlockRequest(request).build();

		Future<DataTransferResponse> f = clientHandler.sendRequestAsync(wrapper);

		Collection<MBlockChunk> chunks = new ArrayList<>();
		CRC32 crc = new CRC32();

		try {
			DataTransferResponse response = f.get();
			LOG.debug("Response: {}", response);

			ReadBlockResponse a = response.getReadBlockResponse();
			for (MBlockChunk chunk : a.getBlockChunkList()) {
				crc.update(chunk.getData().asReadOnlyByteBuffer());
			}
			Collection<MBlockChunk> rChunks = a.getBlockChunkList();

			chunks.addAll(rChunks);

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		LOG.warn("Chunk CRC: {}", crc.getValue());
		return chunks;
	}

	@Override
	public void request(URI node, DeleteBlockRequest request) throws IOException {
		DataTransferRequest wrapper = DataTransferRequest.newBuilder()
				.setId(ByteString.copyFromUtf8(UUID.randomUUID().toString())).setDeleteBlockRequest(request).build();

		ChannelFuture connection = this.connectionFactory.getConnection(node);

		MdfsDataTransferClientHandler clientHandler = connection.channel().pipeline()
				.get(MdfsDataTransferClientHandler.class);

		Future<DataTransferResponse> f = clientHandler.sendRequestAsync(wrapper);

		try {
			DataTransferResponse response = f.get();
			LOG.debug("Response: {}", response);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws Exception {
		this.connectionFactory.close();
	}
}
