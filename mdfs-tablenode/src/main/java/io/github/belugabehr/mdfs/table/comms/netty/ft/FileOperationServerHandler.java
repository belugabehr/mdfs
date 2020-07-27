package io.github.belugabehr.mdfs.table.comms.netty.ft;

import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.github.belugabehr.mdfs.api.FileOperations.FileOperationRequest;
import io.github.belugabehr.mdfs.api.FileOperations.FileOperationResponse;
import io.github.belugabehr.mdfs.table.file.FileManager;
import io.github.belugabehr.mdfs.table.wal.WriteAheadLog;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FileOperationServerHandler extends SimpleChannelInboundHandler<FileOperationRequest> {

	private static final Logger LOG = LoggerFactory.getLogger(FileOperationServerHandler.class);

	@Autowired
	private FileManager fileManager;

	@Autowired
	private WriteAheadLog<Long, FileOperationRequest> wal;

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FileOperationRequest request) throws Exception {
		Future<FileOperationResponse> f = this.fileManager.request(request);

		Long seqno = this.wal.add(request);

		LOG.info("FileOperationRequest [{}]: {}", seqno, request);

		FileOperationResponse response = f.get();
		ctx.writeAndFlush(response);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		ctx.close();
		LOG.error("Error", cause);
	}

}
