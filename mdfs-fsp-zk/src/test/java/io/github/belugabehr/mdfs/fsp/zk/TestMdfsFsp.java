package io.github.belugabehr.mdfs.fsp.zk;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.belugabehr.mdfs.fsp.zk.MdfsZkFileSystemProvider;

public class TestMdfsFsp {
/*
	private static final Logger LOG = LoggerFactory.getLogger(TestMdfsFsp.class);

	@BeforeClass
	public static void setup() throws IOException {
		URI rootURI = URI.create("mdfs://test-cluster");
		MdfsZkFileSystemProvider.instance().newFileSystem(rootURI,
				Collections.singletonMap("hosts", Collections.singleton("127.0.0.1")));
	}

	@Test
	public void testCopyLocalToRemote() throws IOException {
		Path srcPath = Path.of("/home/apache/impossibly-cute-puppy-2.jpg");
		URI rootURI = URI.create("mdfs://test-cluster/big-data/14245f31-0ef7-4379-9b9a-304388f3fb12");
		Path dstPath = Paths.get(rootURI);

		Files.copy(srcPath, dstPath);
	}

	@Test
	public void testCopyRemoteToLocal() throws IOException {
		Path dstPath = Path.of("/home/apache/impossibly-cute-puppy-2-6.jpg");
		Files.deleteIfExists(dstPath);
		URI rootURI = URI.create("mdfs://test-cluster/big-data/14245f31-0ef7-4379-9b9a-304388f3fb11");
		Path srcPath = Paths.get(rootURI);

		Files.copy(srcPath, dstPath);
	}

	@Test
	public void list() throws IOException {
		URI rootURI = URI.create("mdfs://test-cluster/big-data");
		Path srcPath = Paths.get(rootURI);
		try (DirectoryStream<Path> paths = Files.newDirectoryStream(srcPath)) {
			paths.forEach(path -> LOG.warn("Path: {}", path));
		}
	}
	*/

}
