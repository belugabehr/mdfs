package io.github.belugabehr.mdfs.client.file;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.belugabehr.mdfs.api.FileOperations.FileOperationRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FileOperationResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

class MdfsFileOperationClientHandler extends SimpleChannelInboundHandler<FileOperationResponse> {
	private static final Logger LOG = LoggerFactory.getLogger(MdfsFileOperationClientHandler.class);
	private Channel channel;
	private BlockingQueue<FileOperationResponse> responses = new LinkedBlockingQueue<>();

	public MdfsFileOperationClientHandler sendRequest(FileOperationRequest request) {
		channel.writeAndFlush(request);
		return this;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) {
		channel = ctx.channel();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FileOperationResponse msg) throws Exception {
		responses.add(msg);
	}

	public FileOperationResponse getResponse() {
		try {
			return this.responses.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		LOG.error("Error", cause);
		ctx.close();
	}
}
