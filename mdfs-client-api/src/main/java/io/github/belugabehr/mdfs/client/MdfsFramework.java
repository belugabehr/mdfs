package io.github.belugabehr.mdfs.client;

import io.github.belugabehr.mdfs.client.op.CreateBuilder;
import io.github.belugabehr.mdfs.client.op.DeleteBuilder;
import io.github.belugabehr.mdfs.client.op.ExistsBuilder;
import io.github.belugabehr.mdfs.client.op.GetDataBuilder;
import io.github.belugabehr.mdfs.client.op.ListFilesBuilder;

public interface MdfsFramework extends AutoCloseable {

	CreateBuilder create();

	GetDataBuilder getData();

	ListFilesBuilder listFiles();
	
	ExistsBuilder checkExists();
	
	DeleteBuilder delete();

	boolean isOpen();

}
