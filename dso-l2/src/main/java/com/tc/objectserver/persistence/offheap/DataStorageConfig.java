/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package com.tc.objectserver.persistence.offheap;

import com.tc.logging.CustomerLogging;
import com.tc.logging.TCLogger;
import com.tc.objectserver.persistence.StorageManagerFactory;
import com.tc.properties.TCPropertiesConsts;
import com.tc.properties.TCPropertiesImpl;
import com.tc.util.Conversion;
import com.terracottatech.corestorage.bigmemory.BigMemoryKeyValueStorageConfig;

/**
 *
 * @author mscott
 */
public class DataStorageConfig extends OffHeapConfig {
  private static final TCLogger CONSOLE_LOGGER = CustomerLogging.getConsoleLogger();

  private final boolean                 hybridEnabled;
  private final long                    maxDatasize;
  private final boolean                 useMapPartials;
  private final boolean                 useObjectPartials;
  private final BigMemoryKeyValueStorageConfig.OffHeapMode mapMode;
  private final BigMemoryKeyValueStorageConfig.OffHeapMode objectMode;
  
  
  public DataStorageConfig(boolean offHeapEnabled, String maxOffHeapSize) {
    this(offHeapEnabled, maxOffHeapSize, false, maxOffHeapSize,TCPropertiesImpl.getProperties()
        .getBoolean(TCPropertiesConsts.L2_OFFHEAP_SKIP_JVMARG_CHECK, false));
  }
   
  public DataStorageConfig(boolean offHeapEnabled, String maxOffHeapSize, boolean skipJVMArgs) {
    this(offHeapEnabled, maxOffHeapSize, false, maxOffHeapSize,skipJVMArgs);
  }

  public DataStorageConfig(boolean offHeapEnabled, String maxOffHeapSize, boolean hybridEnabled, String maxDataSize) {
    this(offHeapEnabled, maxOffHeapSize, hybridEnabled, maxDataSize,TCPropertiesImpl.getProperties()
        .getBoolean(TCPropertiesConsts.L2_OFFHEAP_SKIP_JVMARG_CHECK, false));
  }  
  
  public DataStorageConfig(boolean offHeapEnabled, String maxOffHeapSize, boolean hybridEnabled, String maxDataSize, boolean skipJVMArgs) {
    super(offHeapEnabled, maxOffHeapSize, skipJVMArgs);
    this.hybridEnabled = hybridEnabled;
    this.maxDatasize = safeConvert(maxDataSize);
    this.useMapPartials = !TCPropertiesImpl.getProperties().getBoolean(TCPropertiesConsts.L2_ALLOCATION_DISABLE_PARTIAL_MAPS, false);
    this.useObjectPartials = !TCPropertiesImpl.getProperties().getBoolean(TCPropertiesConsts.L2_ALLOCATION_DISABLE_PARTIAL_OBJECTS, false);
    objectMode = TCPropertiesImpl.getProperties().getBoolean(TCPropertiesConsts.L2_ALLOCATION_ENABLE_OBJECTS_HOTSET, false)
      ? BigMemoryKeyValueStorageConfig.OffHeapMode.PARTIAL : BigMemoryKeyValueStorageConfig.OffHeapMode.KEYS_ONLY;
    mapMode = TCPropertiesImpl.getProperties().getBoolean(TCPropertiesConsts.L2_ALLOCATION_DISABLE_MAPS_HOTSET, false)
      ? BigMemoryKeyValueStorageConfig.OffHeapMode.MINIMAL : BigMemoryKeyValueStorageConfig.OffHeapMode.PARTIAL;
    if (!hybridEnabled && this.maxDatasize > getOffheapSize()) {
      CONSOLE_LOGGER.warn("Configured dataStorage size " + maxDataSize + " cannot be fulfilled with configured offheap of "
                           + maxOffHeapSize + " and hybrid disabled.");
      CONSOLE_LOGGER.warn("Actual dataStorage will be limited to " + maxOffHeapSize + ". Enable hybrid or increase allocated offheap to fix this.");
    }
  }

  private static long safeConvert(String size) {
    try {
      return Conversion.memorySizeAsLongBytes(size);
    } catch (Conversion.MetricsFormatException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean useHybrid() {
    return hybridEnabled;
  }
  
  public boolean useMapPartials() {
      return this.hybridEnabled && this.useMapPartials;
  }
  
  public boolean useObjectPartials() {
      return this.hybridEnabled && this.useObjectPartials;
  }
  
  public long getMaxDataStorageSize() {
    return this.maxDatasize;
  }
  
  public BigMemoryKeyValueStorageConfig.OffHeapMode getMapMode() {
    if ( this.hybridEnabled && this.useMapPartials ) {
      return mapMode;
    } else {
      return BigMemoryKeyValueStorageConfig.OffHeapMode.FULL;
    }
  }
    
  public BigMemoryKeyValueStorageConfig.OffHeapMode getObjectMode(StorageManagerFactory.Type nt) {
    if ( this.hybridEnabled && this.useObjectPartials && nt == StorageManagerFactory.Type.LEAF ) {
      return objectMode;
    } else {
      return BigMemoryKeyValueStorageConfig.OffHeapMode.FULL;
    }
  }
}
