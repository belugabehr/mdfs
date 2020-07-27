package io.github.belugabehr.mdfs.table.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

public class CachingInputFile implements InputFile {

	private final Path path;
	private long fileSize;
	private final Cache<CachingInputFileKey, ByteBuffer> cache;

	private CachingInputFile(Path path) {
		this.path = path;
		this.fileSize = -1L;
		this.cache = CacheBuilder.newBuilder().maximumWeight(1024 * 1024)
				.weigher(new Weigher<CachingInputFileKey, ByteBuffer>() {
					public int weigh(CachingInputFileKey k, ByteBuffer g) {
						return 16 + g.capacity();
					}
				}).recordStats().build();
	}

	@Override
	public SeekableInputStream newStream() throws IOException {
		return new CachingSeekableInputStream(this.path, this.cache);
	}

	@Override
	public long getLength() throws IOException {
		if (this.fileSize < 0L) {
			this.fileSize = Files.size(this.path);
		}
		return this.fileSize;
	}

	public static CachingInputFile fromPath(Path path) {
		return new CachingInputFile(path);
	}

}
