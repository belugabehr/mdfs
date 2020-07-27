package io.github.belugabehr.mdfs.table;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "cluster")
public class ClusterProperties {
	private List<String> datanodes;

	public List<String> getDatanodes() {
		return datanodes;
	}

	public void setDatanodes(List<String> datanodes) {
		this.datanodes = datanodes;
	}

	@Override
	public String toString() {
		return "ClusterProperties [datanodes=" + datanodes + "]";
	}

}
