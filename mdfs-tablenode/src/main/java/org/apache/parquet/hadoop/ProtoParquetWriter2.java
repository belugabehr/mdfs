package org.apache.parquet.hadoop;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.proto.ProtoWriteSupport;

import com.google.protobuf.MessageOrBuilder;

import io.github.belugabehr.mdfs.table.utils.ParquetOutputFile;

public class ProtoParquetWriter2<T extends MessageOrBuilder> extends ParquetWriter<T> {

	public ProtoParquetWriter2(OutputFile file, Class<T> clazz) throws IOException {
		super(file, ParquetFileWriter.Mode.CREATE, new ProtoWriteSupport(clazz), DEFAULT_COMPRESSION_CODEC_NAME,
				DEFAULT_BLOCK_SIZE, DEFAULT_IS_VALIDATING_ENABLED, new Configuration(), MAX_PADDING_SIZE_DEFAULT,
				ParquetProperties.builder().build());

	}

	public ProtoParquetWriter2(Path path, Class<T> clazz) throws IOException {
		this(new ParquetOutputFile(path), clazz);
	}

}
