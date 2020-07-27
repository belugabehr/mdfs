package io.github.belugabehr.mdfs.datanode.util.ex;

import java.io.IOException;

public class BlockNotFoundException extends IOException {
  public BlockNotFoundException() {
    super("Could not find block in metastore");
  }
}
