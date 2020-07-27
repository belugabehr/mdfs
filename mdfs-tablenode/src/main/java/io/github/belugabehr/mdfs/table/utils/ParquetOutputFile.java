package io.github.belugabehr.mdfs.table.utils;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class ParquetOutputFile implements OutputFile {

	private final Path path;

	public ParquetOutputFile(Path path) throws IOException {
		this.path = Objects.requireNonNull(path);
	}

	@Override
	public PositionOutputStream create(long blockSizeHint) throws IOException {
		SeekableByteChannel channel = Files.newByteChannel(this.path, StandardOpenOption.WRITE);
		return new ParquetPositionOutputStream(channel);
	}

	@Override
	public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean supportsBlockSize() {
		return false;
	}

	@Override
	public long defaultBlockSize() {
		return 0;
	}

}
