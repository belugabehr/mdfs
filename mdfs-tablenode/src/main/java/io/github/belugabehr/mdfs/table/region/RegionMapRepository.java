package io.github.belugabehr.mdfs.table.region;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.PostConstruct;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.protobuf.ByteString;

import io.github.belugabehr.mdfs.fsp.zk.MdfsZkFileSystemProvider;
import io.github.belugabehr.mdfs.table.utils.RowIdByteString;

@Service
public class RegionMapRepository {

	private static final Logger LOG = LoggerFactory.getLogger(RegionMapRepository.class);

	private static String SERVER_ID = UUID.randomUUID().toString();

	private RangeMap<RowIdByteString, Region> regionMap = TreeRangeMap.create();

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	private final Lock readLock = lock.readLock();
	private final Lock writeLock = lock.writeLock();

	@Value("${tablenode.fs.root}")
	private String fsRoot;

	@Autowired
	private CuratorFramework client;

	@PostConstruct
	public void init() throws IOException {
		URI rootNamespace = URI.create(this.fsRoot);
		URI serverNamespace = URI.create(rootNamespace.toString() + "/" + SERVER_ID);

		Map<String, ?> env = Collections.singletonMap("curator", client);
		MdfsZkFileSystemProvider.instance().newFileSystem(serverNamespace, env);

		URI rootURI = URI.create("mdfs-zk://mdfs/");

		LOG.info("rootNamespace: [{}], rootURI: [{}]", rootNamespace, rootURI);

		this.regionMap.put(Range.all(), Region.newBuilder().withRoot(rootURI).build());
	}

	public Region get(final ByteString key) {
		return this.regionMap.get(RowIdByteString.wrap(key));
	}

	public Collection<Region> get(final ByteString minKey, final ByteString maxKey) {
		return new ArrayList<>(
				this.regionMap.subRangeMap(Range.closed(RowIdByteString.wrap(minKey), RowIdByteString.wrap(maxKey)))
						.asMapOfRanges().values());
	}

	public void readLock() {
		this.readLock.lock();
	}

	public void readUnlock() {
		this.readLock.unlock();
	}

	public void writeLock() {
		this.writeLock.lock();
	}

	public void writeUnlock() {
		this.writeLock.unlock();
	}

}
