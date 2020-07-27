package io.github.belugabehr.mdfs.datanode.comms.netty.dt;

import java.io.Closeable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import io.github.belugabehr.mdfs.datanode.IpcProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

@Service
public class DataTransferServer implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(DataTransferServer.class);

	@Autowired
	private MeterRegistry meterRegistry;

	@Autowired
	private IpcProperties ipcProperties;

	@Autowired
	private DataTransferServerInitializer initializer;

	@Autowired
	@Qualifier("dtServerBootstrap")
	private ServerBootstrap serverBootstrap;

	@Autowired
	@Qualifier("dtBossGroup")
	private NioEventLoopGroup bossGroup;

	@Autowired
	@Qualifier("dtWorkerGroup")
	private NioEventLoopGroup workerGroup;

	@Autowired
	private ConnectionCounterChannelHandler connectionCounterChannelHandler;

	private ChannelFuture serverChannel;

	@PostConstruct
	public void init() throws InterruptedException {
		this.serverBootstrap.group(this.bossGroup, this.workerGroup).channel(NioServerSocketChannel.class)
				.childHandler(initializer);
//        .childAttr(AttributeKey.valueOf("storageManager"), this.storageManager);

//    this.meterRegistry.gauge(Metrics.IPC_XCEIVER_TOTAL_COUNT.registryName(), workerGroup, wg -> wg.executorCount());
//
//    this.meterRegistry.gauge(Metrics.IPC_XCEIVER_ACTIVE_COUNT.registryName(),
//        this.connectionCounterChannelHandler.getConnectionCount());

		LOG.warn("Bound to port: {}", ipcProperties.getPort());
		this.serverChannel = this.serverBootstrap.bind(ipcProperties.getPort()).sync();
	}

	@PreDestroy
	public void close() {
		LOG.info("Shutting down Data Transfer Server");
		try {
			// Wait until the server socket is closed
			// this.serverChannel.channel().closeFuture().sync();
			Thread.sleep(15L);

			this.workerGroup.shutdownGracefully();
			this.bossGroup.shutdownGracefully();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.warn("Data Transfer Server shutdown interrupted");
		} finally {
			LOG.info("Shutting down Data Transfer Server complete");
		}
	}
}
