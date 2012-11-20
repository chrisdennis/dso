/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracotta.toolkit.express;

import com.terracotta.toolkit.express.loader.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

class TerracottaInternalClientImpl implements TerracottaInternalClient {

  private static final String TOOLKIT_CONTENT_RESOURCE                                         = "/toolkit-content.txt";
  private static final String SPI_INIT                                                         = "com.terracotta.toolkit.express.SpiInit";
  public static final String  SECRET_PROVIDER                                                  = "com.terracotta.express.SecretProvider";

  private static final String EE_SECRET_DELEGATE                                               = "com.terracotta.toolkit.DelegatingSecretProvider";
  private static final String SECRET_PROVIDER_CLASS                                            = "org.terracotta.toolkit.SecretProvider";

  private static final String EHCACHE_EXPRESS_ENTERPRISE_TERRACOTTA_CLUSTERED_INSTANCE_FACTORY = "net.sf.ehcache.terracotta.ExpressEnterpriseTerracottaClusteredInstanceFactory";
  private static final String EHCACHE_EXPRESS_TERRACOTTA_CLUSTERED_INSTANCE_FACTORY            = "net.sf.ehcache.terracotta.StandaloneTerracottaClusteredInstanceFactory";

  static class ClientShutdownException extends Exception {
    //
  }

  private final ClusteredStateLoader            clusteredStateLoader;
  private final AppClassLoader                  appClassLoader;
  private final DSOContextControl               contextControl;
  private final AtomicInteger                   refCount = new AtomicInteger(0);
  private final TerracottaInternalClientFactory parent;
  private final String                          tcConfig;
  private final boolean                         isUrlConfig;
  private final boolean                         rejoinEnabled;
  private boolean                               shutdown = false;

  TerracottaInternalClientImpl(String tcConfig, boolean isUrlConfig, ClassLoader appLoader,
                               TerracottaInternalClientFactory parent, boolean rejoinEnabled,
                               Set<String> tunneledMBeanDomains, Map<String, Object> env) {
    this.rejoinEnabled = rejoinEnabled;
    this.tcConfig = tcConfig;
    this.isUrlConfig = isUrlConfig;
    this.parent = parent;

    try {
      this.appClassLoader = new AppClassLoader(appLoader);
      this.clusteredStateLoader = createClusteredStateLoader(appLoader);

      Class bootClass = clusteredStateLoader.loadClass(StandaloneL1Boot.class.getName());
      Constructor<?> cstr = bootClass.getConstructor(String.class, Boolean.TYPE, ClassLoader.class, Boolean.TYPE,
                                                     Map.class);

      // XXX: It's be nice to not use Object here, but exposing the necessary type (DSOContext) seems wrong too)
      if (isUrlConfig && isRequestingSecuredEnv(tcConfig)) {
        if (env != null) {
          env.put("com.terracotta.SecretProvider",
                  newSecretProviderDelegate(clusteredStateLoader, env.get(TerracottaInternalClientImpl.SECRET_PROVIDER)));
        }
      }
      Callable<Object> boot = (Callable<Object>) cstr.newInstance(tcConfig, isUrlConfig, clusteredStateLoader,
                                                                  rejoinEnabled, env);

      Object context = boot.call();

      Class spiInit = clusteredStateLoader.loadClass(SPI_INIT);
      contextControl = (DSOContextControl) spiInit.getConstructor(Object.class).newInstance(context);
      join(tunneledMBeanDomains);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isDedicatedClient() {
    return rejoinEnabled;
  }

  @Override
  public synchronized void join(Set<String> tunnelledMBeanDomains) throws ClientShutdownException {
    if (shutdown) throw new ClientShutdownException();
    refCount.incrementAndGet();
    contextControl.activateTunnelledMBeanDomains(tunnelledMBeanDomains);
  }

  @Override
  public <T> T instantiate(String className, Class[] cstrArgTypes, Object[] cstrArgs) throws Exception {
    Class clazz = clusteredStateLoader.loadClass(className);
    Constructor cstr = clazz.getConstructor(cstrArgTypes);
    return (T) cstr.newInstance(cstrArgs);
  }

  @Override
  public synchronized boolean isShutdown() {
    return shutdown;
  }

  @Override
  public synchronized void shutdown() {
    final boolean shutdownClient;
    if (isDedicatedClient()) {
      shutdownClient = true;
    } else {
      // decrement the reference counter by 1 as its shared client
      // destroy real client when count == 0;
      int count = refCount.decrementAndGet();
      if (count < 0) {
        //
        throw new IllegalStateException(
                                        "shutdown() called too many times, this represents a bug in the caller. count = "
                                            + count);
      }
      shutdownClient = count == 0;
    }

    if (shutdownClient) {
      shutdown = true;
      try {
        contextControl.shutdown();
      } finally {
        appClassLoader.clear();
        parent.remove(this, tcConfig, isUrlConfig);
      }
    }
  }

  private byte[] getClassBytes(Class klass) {
    ClassLoader loader = getClass().getClassLoader();
    String res = klass.getName().replace('.', '/').concat(".class");
    try {
      return Util.extract(loader.getResourceAsStream(res));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isEmbeddedEhcacheRequired() {
    // ehcache-core needs to be around
    try {
      Class.forName("net.sf.ehcache.CacheManager");
    } catch (ClassNotFoundException e) {
      return true;
    }
    // One of the ClusteredInstanceFactory classes need to be around (ehcache-terracotta)
    try {
      Class.forName(EHCACHE_EXPRESS_TERRACOTTA_CLUSTERED_INSTANCE_FACTORY);
      return false;
    } catch (ClassNotFoundException e) {
      try {
        Class.forName(EHCACHE_EXPRESS_ENTERPRISE_TERRACOTTA_CLUSTERED_INSTANCE_FACTORY);
        return false;
      } catch (ClassNotFoundException e2) {
        return true;
      }
    }
  }

  private List<String> loadPrefixes() {
    InputStream in = TerracottaInternalClientImpl.class.getResourceAsStream(TOOLKIT_CONTENT_RESOURCE);
    if (in == null) throw new RuntimeException("Couldn't load resource entries file at: " + TOOLKIT_CONTENT_RESOURCE);
    BufferedReader reader = null;
    try {
      List<String> entries = new ArrayList<String>();
      reader = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.length() > 0) {
          if (line.endsWith("/")) {
            entries.add(line);
          } else {
            entries.add(line + "/");
          }
        }
      }
      Collections.sort(entries);
      return entries;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      Util.closeQuietly(in);
    }
  }

  private ClusteredStateLoader createClusteredStateLoader(ClassLoader appLoader) {
    List<String> prefixes = loadPrefixes();

    // we don't need to use embedded ehcache classes if it's already on classpath
    if (!isEmbeddedEhcacheRequired()) {
      for (Iterator<String> it = prefixes.iterator(); it.hasNext();) {
        String prefix = it.next();
        if (prefix.contains("ehcache/")) {
          it.remove();
        }
      }
    }

    ClusteredStateLoader loader = new ClusteredStateLoader(prefixes, appClassLoader);

    loader.addExtraClass(SpiInit.class.getName(), getClassBytes(SpiInit.class));
    loader.addExtraClass(StandaloneL1Boot.class.getName(), getClassBytes(StandaloneL1Boot.class));
    return loader;
  }

  private static boolean isRequestingSecuredEnv(final String tcConfig) {
    return tcConfig.contains("@");
  }

  private static Object newSecretProviderDelegate(final ClassLoader loader, final Object backEnd) {
    try {
      Class customClass = Class.forName(SECRET_PROVIDER_CLASS);
      Class tkClass = loader.loadClass(EE_SECRET_DELEGATE);
      return tkClass.getConstructor(customClass).newInstance(backEnd);
    } catch (Exception e) {
      throw new RuntimeException("Couldn't wrap the custom impl. " + backEnd.getClass().getName()
                                 + " in an instance of " + EE_SECRET_DELEGATE, e);
    }
  }

  @Override
  public boolean isOnline() {
    return contextControl.isOnline();
  }
}
