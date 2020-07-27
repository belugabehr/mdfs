package io.github.belugabehr.mdfs.datanode.storage;

import java.io.IOException;
import java.util.Collection;

public interface VolumeChoosingPolicy
{
    Volume chooseVolume(Collection<Volume> volumes, int dataSize) throws IOException;
}
