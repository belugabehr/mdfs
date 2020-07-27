package io.github.belugabehr.mdfs.datanode.comms.netty.dt;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.DataTransferOperations.DataTransferRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.DataTransferResponse;
import io.github.belugabehr.mdfs.api.DataTransferOperations.DeleteBlockRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.ReadBlockRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.ReadBlockResponse;
import io.github.belugabehr.mdfs.api.DataTransferOperations.WriteBlockChunksRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.WriteBlockRequest;
import io.github.belugabehr.mdfs.api.Mdfs.MBlockChunk;
import io.github.belugabehr.mdfs.datanode.DataNode.LocalMBlock;
import io.github.belugabehr.mdfs.datanode.block.BlockService;
import io.github.belugabehr.mdfs.datanode.util.BlockFileIterator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DataTransferServerHandler extends SimpleChannelInboundHandler<DataTransferRequest> {

	@Autowired
	private BlockService blockService;

	private Optional<LocalMBlock> localBlock = Optional.empty();

	private static final Logger LOG = LoggerFactory.getLogger(DataTransferServerHandler.class);

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, DataTransferRequest request) throws Exception {
		DataTransferResponse.Builder response = DataTransferResponse.newBuilder().setId(request.getId());
		switch (request.getRequestCase()) {
		case READBLOCKREQUEST:
			ReadBlockResponse rbr = handle(ctx, request.getReadBlockRequest());
			response.setReadBlockResponse(rbr);
			break;
		case WRITEBLOCKREQUEST:
			handle(ctx, request.getWriteBlockRequest());
			break;
		case WRITEBLOCKCHUNKSREQUEST:
			handle(ctx, request.getWriteBlockChunksRequest());
			break;
		case DELETEBLOCKREQUEST:
			handle(ctx, request.getDeleteBlockRequest());
			break;
		case REPLICATEBLOCKREQUEST:
			LOG.warn("ReplicateBlock: {}", request.getReplicateBlockRequest());
			break;
		case REQUEST_NOT_SET:
		default:
			throw new RuntimeException("Invalid request type");
		}
		LOG.info("Sending response: {}", response);
		ctx.writeAndFlush(response.build());
	}

	protected void handle(ChannelHandlerContext ctx, DeleteBlockRequest deleteBlockRequest) throws IOException {
		this.blockService.deleteBlock(deleteBlockRequest);
	}

	protected ReadBlockResponse handle(ChannelHandlerContext ctx, ReadBlockRequest request) throws IOException {
		LOG.warn("ReadBlock: {}", request);

		ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
		try (BlockFileIterator chunkIter = blockService.readBlock(request)) {
			int remaining = request.getChunkStop() - request.getChunkStart();
			chunkIter.skip(request.getChunkStart());
			while (chunkIter.hasNext() && remaining-- > 0) {
				response.addBlockChunk(chunkIter.next());
			}
		}
		return response.build();
	}

	protected void handle(ChannelHandlerContext ctx, WriteBlockRequest request) throws IOException {
		LOG.warn("WriteBlock: {}", request);
		LocalMBlock localBlock = blockService.writeBlock(request);
		this.localBlock = Optional.of(localBlock);
	}

	protected void handle(ChannelHandlerContext ctx, WriteBlockChunksRequest request) throws IOException {
		LOG.warn("WriteBlockChunk: {}", request);
		String filePathURI = this.localBlock.get().getLocation();
		Path path = Paths.get(URI.create(filePathURI));

		try (OutputStream fos = new BufferedOutputStream(new FileOutputStream(path.toFile(), true))) {
			for (MBlockChunk chunk : request.getChunkList()) {
				LOG.info("Writing chunk");
				chunk.writeDelimitedTo(fos);
			}
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOG.error("Closing DataTransferServerHandler", cause);
		ctx.close();
	}

}
