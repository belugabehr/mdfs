package io.github.belugabehr.mdfs.fsp.zk;

import java.nio.file.FileSystem;
import java.nio.file.Path;

import org.junit.Test;
import org.mockito.Mockito;

public class TestMdfsZkPath {

	@Test
	public void testResolveString() {
		FileSystem fs = Mockito.mock(FileSystem.class);

		Path path = new MdfsZkPath(fs, "/server/my/path");
		Path path2 = path.resolve("test");
		
		System.out.println(path);
		System.out.println(path2);
	}

}
