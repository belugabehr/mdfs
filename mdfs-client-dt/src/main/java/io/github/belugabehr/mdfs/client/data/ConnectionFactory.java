package io.github.belugabehr.mdfs.client.data;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

class ConnectionFactory implements AutoCloseable {
	
	private static final Logger LOG = LoggerFactory.getLogger(ConnectionFactory.class);

	private final EventLoopGroup workerGroup = new NioEventLoopGroup();

	private final LoadingCache<URI, ChannelFuture> channelCache;

	ConnectionFactory() {
		this.channelCache = CacheBuilder.newBuilder().expireAfterWrite(2, TimeUnit.MINUTES)
				.removalListener(removalListener()).build(cacheLoader());
	}

	ChannelFuture getConnection(URI uri) throws IOException {
		try {
			return this.channelCache.get(uri);
		} catch (ExecutionException e) {
			throw new IOException(e.getCause());
		}
	}

	protected ChannelFuture openConnection(URI uri) throws IOException, InterruptedException {
		Bootstrap b = new Bootstrap();
		b.group(workerGroup);
		b.channel(NioSocketChannel.class);
		b.option(ChannelOption.SO_KEEPALIVE, true);
		b.handler(new MdfsDataTransferClientInitializer());

		// Start the client.
		LOG.warn("URI [{}][{}]", uri, uri.getPort());
		ChannelFuture channelFuture = b.connect(uri.getHost(), uri.getPort()).sync();
		return channelFuture;
	}

	protected CacheLoader<URI, ChannelFuture> cacheLoader() {
		return new CacheLoader<URI, ChannelFuture>() {
			public ChannelFuture load(URI uri) throws Exception {
				return openConnection(uri);
			}
		};
	}

	protected RemovalListener<URI, ChannelFuture> removalListener() {
		return new RemovalListener<URI, ChannelFuture>() {
			public void onRemoval(RemovalNotification<URI, ChannelFuture> removal) {
				ChannelFuture f = removal.getValue();
				try {
					// TODO: Maybe don't bother sync-ing here. Let it close when it closes.
					f.channel().close().sync();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
	}

	@Override
	public void close() throws Exception {
		this.channelCache.invalidateAll();
		workerGroup.shutdownGracefully();
	}
}
