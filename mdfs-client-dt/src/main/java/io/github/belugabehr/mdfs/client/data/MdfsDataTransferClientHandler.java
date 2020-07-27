package io.github.belugabehr.mdfs.client.data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.api.DataTransferOperations.DataTransferRequest;
import io.github.belugabehr.mdfs.api.DataTransferOperations.DataTransferResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

class MdfsDataTransferClientHandler extends SimpleChannelInboundHandler<DataTransferResponse> {
	private static final Logger LOG = LoggerFactory.getLogger(MdfsDataTransferClientHandler.class);
	private Channel channel;
	private final Map<ByteString, DataTransferRequestFuture> futuresMap = new ConcurrentHashMap<>();

	public Future<DataTransferResponse> sendRequestAsync(DataTransferRequest request) {
		DataTransferRequestFuture future = new DataTransferRequestFuture();
		this.futuresMap.put(request.getId(), future);

		channel.writeAndFlush(request);

		return future;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) {
		channel = ctx.channel();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, DataTransferResponse msg) throws Exception {
		LOG.warn("Response came in");
		DataTransferRequestFuture future = this.futuresMap.get(msg.getId());
		future.setResponse(msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		LOG.error("Error", cause);
		ctx.close();
	}
}
