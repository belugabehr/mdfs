package io.github.belugabehr.mdfs.datanode.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

@Service
public class StorageManager implements VolumeGroupChangeListener {

  private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);

  private static final Path NAMESPACE = Path.of("dfs");

  @Autowired
  private MeterRegistry meterRegisty;

  private StorageProperties storageProperties;

  private VolumeGroupInitializer volumeGroupInitializer;

  private VolumeInitializer volumeInitializer;

  @Autowired
  @Qualifier("globalScheduledTaskExecutor")
  private ScheduledExecutorService scheduler;

  private final Map<UUID, VolumeGroup> volumeGroups = Maps.newHashMap();

  private final Map<UUID, Volume> volumes = Maps.newHashMap();

  private final List<VolumeGroupWatcher> watchers = Lists.newArrayList();

//  @PostConstruct
  public void init() throws IOException {
    for (final Entry<String, VolumeGroupProperties> entry : this.storageProperties.getGroups().entrySet()) {
      final String volumeGroupName = entry.getKey();
      final VolumeGroupProperties volumeGroupProperties = entry.getValue();

      LOG.info("Initialize storage: {} [{}]", volumeGroupName, volumeGroupProperties.getDescription());

      final VolumeGroup volumeGroup = this.addVolumeGroup(volumeGroupName, volumeGroupProperties);
      final Path storageDirectory = Paths.get(volumeGroupProperties.getDirectory());

      final Collection<Path> availableVolumes = doDiscoverVolumes(storageDirectory);
      availableVolumes.forEach(volume -> {
        try {
          addVolume(volumeGroup, volume);
        } catch (IOException ioe) {
          LOG.error("Could not initialize and register a volume [{}]. Skipped.", volume, ioe);
        }
      });

      watchers.add(new VolumeGroupWatcher(volumeGroup, scheduler, this).watch());
    }

    this.meterRegisty.gaugeCollectionSize("datanode.storage.groups.count", Tags.empty(), this.volumeGroups.keySet());
    this.meterRegisty.gaugeCollectionSize("datanode.storage.volumes.count", Tags.empty(), this.volumes.keySet());
  }

  public VolumeGroup addVolumeGroup(final String volumeGroupName, final VolumeGroupProperties properties)
      throws IOException {
    final Path mountDirectory = Paths.get(properties.getDirectory());
    final UUID volumeGroupId = this.volumeGroupInitializer.init(mountDirectory);

    final VolumeGroup volumeGroup = new VolumeGroup();
    volumeGroup.setId(volumeGroupId);
    volumeGroup.setName(volumeGroupName);
    volumeGroup.setDescription(properties.getDescription());
    volumeGroup.setMountDirectory(mountDirectory);
    volumeGroup.setReservedSpace(properties.getReserved());

    this.volumeGroups.put(volumeGroupId, volumeGroup);

    return volumeGroup;
  }

  public Volume addVolume(final VolumeGroup volumeGroup, final Path volumePath) throws IOException {
    final Path namespacePath = volumePath.resolve(NAMESPACE);
    final UUID volumeId = this.volumeInitializer.init(namespacePath);
    final Volume volume = new DefaultVolume(volumeId, namespacePath, volumeGroup.getReservedSpace());

    volumeGroup.getVolumes().put(volumeId, volume);
    this.volumes.put(volumeId, volume);

    return volume;
  }

  @Override
  public void volumeAdded(final VolumeGroup volumeGroup, final Path child) throws IOException {
    addVolume(volumeGroup, child);
  }

  public Volume getNextAvailableVolume(final UUID volumeGroupId, final long requestedBlockSize) {
    final Collection<Volume> volumes = this.volumeGroups.get(volumeGroupId).getVolumes().values();
    final int volCount = volumes.size();
    return Iterables.get(volumes, ThreadLocalRandom.current().nextInt(volCount));
  }

  protected Collection<Path> doDiscoverVolumes(final Path dataPath) throws IOException {
    return Files.list(dataPath).filter(Files::isDirectory).collect(Collectors.toList());
  }

  public Optional<Volume> getVolume(final UUID volumeUUID) {
    return Optional.fromNullable(this.volumes.get(volumeUUID));
  }

  public Collection<VolumeGroup> getVolumeGroups() {
    return Collections.unmodifiableCollection(this.volumeGroups.values());
  }

  public Collection<Volume> getAllVolumes() {
    return this.volumeGroups.values().stream().map(volumeGroup -> volumeGroup.getVolumes())
        .flatMap(entry -> entry.values().stream()).filter(volume -> !volume.isFailed()).collect(Collectors.toList());
  }

}
