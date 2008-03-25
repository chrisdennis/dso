/*
 * All content copyright (c) 2003-2006 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.object;

import com.tc.exception.TCClassNotFoundException;
import com.tc.exception.TCNonPortableObjectError;
import com.tc.exception.TCRuntimeException;
import com.tc.logging.ChannelIDLogger;
import com.tc.logging.CustomerLogging;
import com.tc.logging.DumpHandler;
import com.tc.logging.TCLogger;
import com.tc.logging.TCLogging;
import com.tc.net.protocol.tcm.ChannelIDProvider;
import com.tc.object.appevent.ApplicationEvent;
import com.tc.object.appevent.ApplicationEventContext;
import com.tc.object.appevent.NonPortableEventContext;
import com.tc.object.appevent.NonPortableEventContextFactory;
import com.tc.object.appevent.NonPortableFieldSetContext;
import com.tc.object.appevent.NonPortableObjectEvent;
import com.tc.object.appevent.NonPortableRootContext;
import com.tc.object.bytecode.Manageable;
import com.tc.object.bytecode.ManagerUtil;
import com.tc.object.bytecode.TransparentAccess;
import com.tc.object.cache.CacheStats;
import com.tc.object.cache.Evictable;
import com.tc.object.cache.EvictionPolicy;
import com.tc.object.config.DSOClientConfigHelper;
import com.tc.object.dna.api.DNA;
import com.tc.object.idprovider.api.ObjectIDProvider;
import com.tc.object.loaders.ClassProvider;
import com.tc.object.loaders.Namespace;
import com.tc.object.logging.RuntimeLogger;
import com.tc.object.msg.JMXMessage;
import com.tc.object.net.DSOClientMessageChannel;
import com.tc.object.tx.ClientTransactionManager;
import com.tc.object.tx.optimistic.OptimisticTransactionManager;
import com.tc.object.tx.optimistic.TCObjectClone;
import com.tc.object.util.IdentityWeakHashMap;
import com.tc.object.util.ToggleableStrongReference;
import com.tc.object.walker.ObjectGraphWalker;
import com.tc.text.ConsoleNonPortableReasonFormatter;
import com.tc.text.ConsoleParagraphFormatter;
import com.tc.text.NonPortableReasonFormatter;
import com.tc.text.ParagraphFormatter;
import com.tc.text.PrettyPrintable;
import com.tc.text.PrettyPrinter;
import com.tc.text.PrettyPrinterImpl;
import com.tc.text.StringFormatter;
import com.tc.util.Assert;
import com.tc.util.Counter;
import com.tc.util.NonPortableReason;
import com.tc.util.ToggleableReferenceManager;
import com.tc.util.State;
import com.tc.util.Util;
import com.tc.util.concurrent.ResetableLatch;
import com.tc.util.concurrent.StoppableThread;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClientObjectManagerImpl implements ClientObjectManager, PortableObjectProvider, Evictable, DumpHandler,
    PrettyPrintable {

  private static final State                   PAUSED                 = new State("PAUSED");
  private static final State                   STARTING               = new State("STARTING");
  private static final State                   RUNNING                = new State("RUNNING");

  private static final LiteralValues           literals               = new LiteralValues();
  private static final TCLogger                staticLogger           = TCLogging.getLogger(ClientObjectManager.class);

  private static final long                    POLL_TIME              = 1000;
  private static final long                    STOP_WAIT              = POLL_TIME * 3;

  private static final int                     NO_DEPTH               = 0;

  private static final int                     COMMIT_SIZE            = 100;

  private State                                state                  = RUNNING;
  private final Object                         shutdownLock           = new Object();
  private final Map                            roots                  = new HashMap();
  private final Map                            idToManaged            = new HashMap();
  private final Map                            pojoToManaged          = new IdentityWeakHashMap();
  private final ClassProvider                  classProvider;
  private final RemoteObjectManager            remoteObjectManager;
  private final EvictionPolicy                 cache;
  private final Traverser                      traverser;
  private final Traverser                      shareObjectsTraverser;
  private final TraverseTest                   traverseTest;
  private final DSOClientConfigHelper          clientConfiguration;
  private final TCClassFactory                 clazzFactory;
  private final Set                            rootLookupsInProgress  = new HashSet();
  private final ObjectIDProvider               idProvider;
  private final ReferenceQueue                 referenceQueue         = new ReferenceQueue();
  private final TCObjectFactory                factory;

  private ClientTransactionManager             txManager;

  private StoppableThread                      reaper                 = null;
  private final TCLogger                       logger;
  private final RuntimeLogger                  runtimeLogger;
  private final NonPortableEventContextFactory appEventContextFactory;
  private final Set                            pendingCreateTCObjects = new HashSet();
  private final Portability                    portability;
  private final DSOClientMessageChannel        channel;
  private final ToggleableReferenceManager     referenceManager;

  private final boolean                        sendErrors             = System.getProperty("project.name") != null;

  private final Map                            objectLatchStateMap    = new HashMap();
  private final ThreadLocal                    localLookupContext     = new ThreadLocal() {

                                                                        protected synchronized Object initialValue() {
                                                                          return new LocalLookupContext();
                                                                        }

                                                                      };

  public ClientObjectManagerImpl(RemoteObjectManager remoteObjectManager, DSOClientConfigHelper clientConfiguration,
                                 ObjectIDProvider idProvider, EvictionPolicy cache, RuntimeLogger runtimeLogger,
                                 ChannelIDProvider provider, ClassProvider classProvider, TCClassFactory classFactory,
                                 TCObjectFactory objectFactory, Portability portability,
                                 DSOClientMessageChannel channel, ToggleableReferenceManager referenceManager) {
    this.remoteObjectManager = remoteObjectManager;
    this.cache = cache;
    this.clientConfiguration = clientConfiguration;
    this.idProvider = idProvider;
    this.runtimeLogger = runtimeLogger;
    this.portability = portability;
    this.channel = channel;
    this.referenceManager = referenceManager;
    this.logger = new ChannelIDLogger(provider, TCLogging.getLogger(ClientObjectManager.class));
    this.classProvider = classProvider;
    this.traverseTest = new NewObjectTraverseTest();
    this.traverser = new Traverser(new AddManagedObjectAction(), this);
    this.shareObjectsTraverser = new Traverser(new SharedObjectsAction(), this);
    this.clazzFactory = classFactory;
    this.factory = objectFactory;
    this.factory.setObjectManager(this);
    this.appEventContextFactory = new NonPortableEventContextFactory(provider);

    if (logger.isDebugEnabled()) {
      logger.debug("Starting up ClientObjectManager:" + System.identityHashCode(this) + ". Cache SIZE = "
                   + cache.getCacheCapacity());
    }
    startReaper();
    ensureLocalLookupContextLoaded();
  }

  private void ensureLocalLookupContextLoaded() {
    //load LocalLookupContext early to avoid ClassCircularityError: DEV-1386
    new LocalLookupContext();
  }

  public Class getClassFor(String className, String loaderDesc) throws ClassNotFoundException {
    return classProvider.getClassFor(className, loaderDesc);
  }

  public synchronized void pause() {
    assertNotPaused("Attempt to pause while PAUSED");
    state = PAUSED;
    notifyAll();
  }

  public synchronized void starting() {
    assertPaused("Attempt to start while not PAUSED");
    state = STARTING;
    notifyAll();
  }

  public synchronized void unpause() {
    assertStarting("Attempt to unpause while not STARTING");
    state = RUNNING;
    notifyAll();
  }

  public Object createParentCopyInstanceIfNecessary(Map visited, Map cloned, Object v) {
    TCClass tcc = getOrCreateClass(v.getClass());
    Object parent = null;
    if (tcc.isNonStaticInner()) {
      TransparentAccess access = (TransparentAccess) v;
      Map m = new HashMap();
      access.__tc_getallfields(m);
      Object p = m.get(tcc.getParentFieldName());
      parent = visited.containsKey(p) ? visited.get(p) : createNewCopyInstance(p, null);
      visited.put(p, parent);
      cloned.put(p, parent);
    }
    return parent;
  }

  private void waitUntilRunning() {
    boolean isInterrupted = false;

    while (state != RUNNING) {
      try {
        wait();
      } catch (InterruptedException e) {
        isInterrupted = true;
      }
    }
    Util.selfInterruptIfNeeded(isInterrupted);
  }

  private void assertPaused(Object message) {
    if (state != PAUSED) throw new AssertionError(message + ": " + state);
  }

  private void assertStarting(Object message) {
    if (state != STARTING) throw new AssertionError(message + ": " + state);
  }

  private void assertNotPaused(Object message) {
    if (state == PAUSED) throw new AssertionError(message + ": " + state);
  }

  public TraversedReferences getPortableObjects(Class clazz, Object start, TraversedReferences addTo) {
    TCClass tcc = clazzFactory.getOrCreate(clazz, this);
    return tcc.getPortableObjects(start, addTo);
  }

  public void setTransactionManager(ClientTransactionManager txManager) {
    this.txManager = txManager;
  }

  public ClientTransactionManager getTransactionManager() {
    return txManager;
  }

  /**
   * Deep connected copy used to create stable views on collections of objects. While inefficient this should do that
   * job. It is important that this method be called holding a distributed lock in order to prevent an unstable view. It
   * also must be called in an optimistic transaction I'll probably move this out of the client object manager at some
   * point but we'll see.
   */
  public Object deepCopy(Object source, OptimisticTransactionManager optimisticTxManager) {
    IdentityHashMap cloned = new IdentityHashMap();
    IdentityHashMap visited = new IdentityHashMap();

    Object parent = this.createParentCopyInstanceIfNecessary(visited, cloned, source);
    Object copy = createNewCopyInstance(source, parent);

    Assert.eval(copy != null);

    visited.put(source, copy);
    optimisticTxManager.addClonesToTransaction(visited);

    cloneAndUpdate(optimisticTxManager, cloned, visited, source, copy);
    while (!cloned.isEmpty()) {
      Object original = cloned.keySet().iterator().next(); // ick
      Object clone = cloned.get(original);
      cloned.remove(original);
      cloneAndUpdate(optimisticTxManager, cloned, visited, original, clone);
    }

    return copy;
  }

  private LocalLookupContext getLocalLookupContext() {
    return (LocalLookupContext) localLookupContext.get();
  }

  private ObjectLatchState getObjectLatchState(ObjectID id) {
    return (ObjectLatchState) objectLatchStateMap.get(id);
  }

  private ObjectLatchState markLookupInProgress(ObjectID id) {
    ResetableLatch latch = getLocalLookupContext().getLatch();
    ObjectLatchState ols = new ObjectLatchState(id, latch);
    Object old = objectLatchStateMap.put(id, ols);
    Assert.assertNull(old);
    return ols;
  }

  private synchronized void markCreateInProgress(ObjectLatchState ols, TCObject object, LocalLookupContext lookupContext) {
    ResetableLatch latch = lookupContext.getLatch();
    // Make sure this thread owns this object lookup
    Assert.assertTrue(ols.getLatch() == latch);
    ols.setObject(object);
    ols.markCreateState();
    lookupContext.getObjectCreationCount().increment();
  }

  private synchronized void removeCreateInProgress(ObjectID id) {
    objectLatchStateMap.remove(id);
    getLocalLookupContext().getObjectCreationCount().decrement();
  }

  // For testing purposes
  protected Map getObjectLatchStateMap() {
    return objectLatchStateMap;
  }

  /**
   * While holding the resolve lock to protect against the cleaner create a new copy of the original that is connected
   * to the copy and has any references replaced with either an existing clone or a new clone where needed. New clones
   * created in the connected copy are returned so that they can be properly updated from their originals. The reason
   * for this strategy is to avoid recurrsion (and stack over flows)
   */
  private void cloneAndUpdate(OptimisticTransactionManager optimisticTxManager, IdentityHashMap cloned,
                              IdentityHashMap visited, Object original, Object clone) {
    TCClass tcc;
    TCObject tco;
    tcc = this.getOrCreateClass(original.getClass());
    tco = this.lookupExistingOrNull(original);
    synchronized (tco.getResolveLock()) {
      tco.resolveAllReferences();
      Map c = tcc.connectedCopy(original, clone, visited, optimisticTxManager);
      optimisticTxManager.addClonesToTransaction(c);
      cloned.putAll(c);
    }
  }

  private TCObject create(Object pojo, NonPortableEventContext context) {
    addToManagedFromRoot(pojo, context);
    return basicLookup(pojo);
  }

  private TCObject share(Object pojo, NonPortableEventContext context) {
    addToSharedFromRoot(pojo, context);
    return basicLookup(pojo);
  }

  public ReferenceQueue getReferenceQueue() {
    return referenceQueue;
  }

  public void shutdown() {
    synchronized (shutdownLock) {
      if (reaper != null) {
        try {
          stopThread(reaper);
        } finally {
          reaper = null;
        }
      }
    }
  }

  private static void stopThread(StoppableThread thread) {
    try {
      thread.stopAndWait(STOP_WAIT);
    } finally {
      if (thread.isAlive()) {
        staticLogger.warn(thread.getName() + " is still alive");
      }
    }
  }

  public TCObject lookupOrCreate(Object pojo) {
    if (pojo == null) return TCObjectFactory.NULL_TC_OBJECT;
    return lookupOrCreateIfNecesary(pojo, this.appEventContextFactory.createNonPortableEventContext(pojo));
  }

  private TCObject lookupOrCreate(Object pojo, NonPortableEventContext context) {
    if (pojo == null) return TCObjectFactory.NULL_TC_OBJECT;
    return lookupOrCreateIfNecesary(pojo, context);
  }

  public TCObject lookupOrShare(Object pojo) {
    if (pojo == null) return TCObjectFactory.NULL_TC_OBJECT;
    return lookupOrShareIfNecesary(pojo, this.appEventContextFactory.createNonPortableEventContext(pojo));
  }

  private TCObject lookupOrShareIfNecesary(Object pojo, NonPortableEventContext context) {
    Assert.assertNotNull(pojo);
    TCObject obj = basicLookup(pojo);
    if (obj == null || obj.isNew()) {
      obj = share(pojo, context);
    }
    return obj;
  }

  private TCObject lookupOrCreateIfNecesary(Object pojo, NonPortableEventContext context) {
    Assert.assertNotNull(pojo);
    TCObject obj = basicLookup(pojo);
    if (obj == null || obj.isNew()) {
      executePreCreateMethod(pojo);
      obj = create(pojo, context);
    }
    return obj;
  }

  private void executePreCreateMethod(Object pojo) {
    String onLookupMethodName = clientConfiguration.getPreCreateMethodIfDefined(pojo.getClass().getName());
    if (onLookupMethodName != null) {
      executeMethod(pojo, onLookupMethodName, "preCreate method (" + onLookupMethodName + ") failed on object of "
                                              + pojo.getClass());
    }
  }

  /**
   * This method is created for situations in which a method needs to be taken place when an object moved from
   * non-shared to shared. The method could be an instrumented method. For instance, for ConcurrentHashMap, we need to
   * re-hash the objects already in the map because the hashing algorithm is different when a ConcurrentHashMap is
   * shared. The rehash method is an instrumented method. This should be executed only once.
   */
  private void executePostCreateMethod(Object pojo) {
    String onLookupMethodName = clientConfiguration.getPostCreateMethodIfDefined(pojo.getClass().getName());
    if (onLookupMethodName != null) {
      executeMethod(pojo, onLookupMethodName, "postCreate method (" + onLookupMethodName + ") failed on object of "
                                              + pojo.getClass());
    }
  }

  private void executeMethod(Object pojo, String onLookupMethodName, String loggingMessage) {
    // This method used to use beanshell, but I changed it to reflection to hopefully avoid a deadlock -- CDV-130

    try {
      Method m = pojo.getClass().getDeclaredMethod(onLookupMethodName, new Class[] {});
      m.setAccessible(true);
      m.invoke(pojo, new Object[] {});
    } catch (Throwable t) {
      if (t instanceof InvocationTargetException) {
        t = t.getCause();
      }
      logger.warn(loggingMessage, t);
      if (!(t instanceof RuntimeException)) {
        t = new RuntimeException(t);
      }
      throw (RuntimeException) t;
    }
  }

  private TCObject lookupExistingLiteralRootOrNull(String rootName) {
    ObjectID rootID = (ObjectID) roots.get(rootName);
    return basicLookupByID(rootID);
  }

  public TCObject lookupExistingOrNull(Object pojo) {
    return basicLookup(pojo);
  }

  public synchronized ObjectID lookupExistingObjectID(Object pojo) {
    TCObject obj = basicLookup(pojo);
    if (obj == null) { throw new AssertionError("Missing object ID for:" + pojo); }
    return obj.getObjectID();
  }

  public void markReferenced(TCObject tcobj) {
    cache.markReferenced(tcobj);
  }

  public Object lookupObjectNoDepth(ObjectID id) throws ClassNotFoundException {
    return lookupObject(id, null, true);
  }

  public Object lookupObject(ObjectID objectID) throws ClassNotFoundException {
    return lookupObject(objectID, null, false);
  }

  public Object lookupObject(ObjectID id, ObjectID parentContext) throws ClassNotFoundException {
    return lookupObject(id, parentContext, false);
  }

  private Object lookupObject(ObjectID objectID, ObjectID parentContext, boolean noDepth) throws ClassNotFoundException {
    if (objectID.isNull()) return null;
    Object o = null;
    while (o == null) {
      final TCObject tco = lookup(objectID, parentContext, noDepth);
      if (tco == null) throw new AssertionError("TCObject was null for " + objectID);// continue;

      o = tco.getPeerObject();
      if (o == null) {
        reap(objectID);
      }
    }
    return o;
  }

  private void reap(ObjectID objectID) {
    synchronized (this) {
      if (!basicHasLocal(objectID)) {
        if (logger.isDebugEnabled()) logger.debug(System.identityHashCode(this)
                                                  + " Entry removed before reaper got the chance: " + objectID);
      } else {
        TCObjectImpl tcobj = (TCObjectImpl) basicLookupByID(objectID);
        if (tcobj.isNull()) {
          idToManaged.remove(objectID);
          cache.remove(tcobj);
          remoteObjectManager.removed(objectID);
        }
      }
    }
  }

  public boolean isManaged(Object pojo) {
    return pojo != null && !literals.isLiteral(pojo.getClass().getName()) && lookupExistingOrNull(pojo) != null;
  }

  public boolean isCreationInProgress() {
    return getLocalLookupContext().getObjectCreationCount().get() > 0 ? true : false;
  }

  // Done

  public TCObject lookup(ObjectID id) throws ClassNotFoundException {
    return lookup(id, null, false);
  }

  private TCObject lookup(ObjectID id, ObjectID parentContext, boolean noDepth) throws ClassNotFoundException {
    TCObject obj = null;
    boolean retrieveNeeded = false;
    boolean isInterrupted = false;

    LocalLookupContext lookupContext = getLocalLookupContext();

    if (lookupContext.getCallStackCount().increment() == 1) {
      // first time
      txManager.disableTransactionLogging();
      lookupContext.getLatch().reset();
    }

    try {
      ObjectLatchState ols;
      synchronized (this) {
        while (true) {
          ols = getObjectLatchState(id);
          obj = basicLookupByID(id);
          if (obj != null) {
            // object exists in local cache
             return obj;
          } else if (ols != null && ols.isCreateState()) {
            // if the object is being created, add to the wait set and return the object
            lookupContext.getObjectLatchWaitSet().add(ols);
            return ols.getObject();
          } else if (ols != null && ols.isLookupState()) {
            // the object is being looked up, wait.
            try {
              wait();
            } catch (InterruptedException ie) {
              isInterrupted = true;
            }
          } else {
            // otherwise, we need to lookup the object
            retrieveNeeded = true;
            ols = markLookupInProgress(id);
            break;
          }
        }
      }
      Util.selfInterruptIfNeeded(isInterrupted);

      // retrieving object required, first looking up the DNA from the remote server, and creating
      // a pre-init TCObject, then hydrating the object
      if (retrieveNeeded) {
        try {
          DNA dna = noDepth ? remoteObjectManager.retrieve(id, NO_DEPTH) : (parentContext == null ? remoteObjectManager
              .retrieve(id) : remoteObjectManager.retrieveWithParentContext(id, parentContext));
          obj = factory.getNewInstance(id, classProvider.getClassFor(Namespace.parseClassNameIfNecessary(dna
              .getTypeName()), dna.getDefiningLoaderDescription()), false);

          // object is retrieved, now you want to make this as Creation in progress
          markCreateInProgress(ols, obj, lookupContext);

          Assert.assertFalse(dna.isDelta());
          // now hydrate the object, this could call resolveReferences which would call this method recursively
          obj.hydrate(dna, false);
        } catch (ClassNotFoundException e) {
          logger.warn("Exception: ", e);
          throw e;
        } finally {
          // remove the object creating in progress from the list.
          removeCreateInProgress(id);
        }
        basicAddLocal(obj);
      }
    } finally {
      if (lookupContext.getCallStackCount().decrement() == 0) {
        // release your own local latch
        lookupContext.getLatch().release();
        Set waitSet = lookupContext.getObjectLatchWaitSet();
        waitAndClearLatchSet(waitSet);
        // enabled transaction logging
        txManager.enableTransactionLogging();
      }
    }
    return obj;

  }

  private void waitAndClearLatchSet(Set waitSet) {
    boolean isInterrupted = false;
    // now wait till all the other objects you are waiting for releases there latch.
    for (Iterator iter = waitSet.iterator(); iter.hasNext();) {
      ObjectLatchState ols = (ObjectLatchState) iter.next();
      while (true) {
        try {
          ols.getLatch().acquire();
          break;
        } catch (InterruptedException e) {
          isInterrupted = true;
        }
      }

    }
    Util.selfInterruptIfNeeded(isInterrupted);
    waitSet.clear();
  }

  public synchronized TCObject lookupIfLocal(ObjectID id) {
    return basicLookupByID(id);
  }

  public synchronized Collection getAllObjectIDsAndClear(Collection c) {
    assertStarting("Called when not in STARTING state !");
    for (Iterator i = idToManaged.keySet().iterator(); i.hasNext();) {
      c.add(i.next());
    }
    remoteObjectManager.clear();
    return c;
  }

  public Object lookupRoot(String rootName) {
    try {
      return lookupRootOptionallyCreateOrReplace(rootName, null, false, true, false);
    } catch (ClassNotFoundException e) {
      throw new TCClassNotFoundException(e);
    }
  }

  /**
   * Check to see if the root is already in existence on the server. If it is then get it if not then create it.
   */
  public Object lookupOrCreateRoot(String rootName, Object root) {
    try {
      return lookupOrCreateRoot(rootName, root, true, false);
    } catch (ClassNotFoundException e) {
      throw new TCClassNotFoundException(e);
    }
  }

  /**
   * This method must be called within a DSO synchronized context. Currently, this is called in a setter method of a
   * replaceable root.
   */
  public Object createOrReplaceRoot(String rootName, Object root) {
    Object existingRoot = lookupRoot(rootName);
    if (existingRoot == null) {
      return lookupOrCreateRoot(rootName, root, false);
    } else if (isLiteralPojo(root)) {
      TCObject tcObject = lookupExistingLiteralRootOrNull(rootName);
      tcObject.literalValueChanged(root, existingRoot);
      return root;
    } else {
      return lookupOrCreateRoot(rootName, root, false);
    }
  }

  public Object lookupOrCreateRootNoDepth(String rootName, Object root) {
    try {
      return lookupOrCreateRoot(rootName, root, true, true);
    } catch (ClassNotFoundException e) {
      throw new TCClassNotFoundException(e);
    }
  }

  public Object lookupOrCreateRoot(String rootName, Object root, boolean dsoFinal) {
    try {
      return lookupOrCreateRoot(rootName, root, dsoFinal, false);
    } catch (ClassNotFoundException e) {
      throw new TCClassNotFoundException(e);
    }
  }

  private boolean isLiteralPojo(Object pojo) {
    return !(pojo instanceof Class) && literals.isLiteralInstance(pojo);
  }

  private Object lookupOrCreateRoot(String rootName, Object root, boolean dsoFinal, boolean noDepth)
      throws ClassNotFoundException {
    if (root != null) {
      // this will throw an exception if root is not portable
      this.checkPortabilityOfRoot(root, rootName, root.getClass());
    }

    return lookupRootOptionallyCreateOrReplace(rootName, root, true, dsoFinal, noDepth);
  }

  private void checkPortabilityOfTraversedReference(TraversedReference reference, Class referringClass,
                                                    NonPortableEventContext context) {
    NonPortableReason reason = checkPortabilityOf(reference.getValue());
    if (reason != null) {
      reason.addDetail("Referring class", referringClass.getName());
      if (!reference.isAnonymous()) {
        String fullyQualifiedFieldname = reference.getFullyQualifiedReferenceName();
        reason.setUltimateNonPortableFieldName(fullyQualifiedFieldname);
      }
      dumpObjectHierarchy(context.getPojo(), context);
      if (sendErrors) {
        storeObjectHierarchy(context.getPojo(), context);
      }
      throwNonPortableException(context.getPojo(), reason, context,
                                "Attempt to share an instance of a non-portable class referenced by a portable class.");
    }
  }

  private void checkPortabilityOfRoot(Object root, String rootName, Class rootType) throws TCNonPortableObjectError {
    NonPortableReason reason = checkPortabilityOf(root);
    if (reason != null) {
      NonPortableRootContext context = this.appEventContextFactory.createNonPortableRootContext(rootName, root);
      dumpObjectHierarchy(root, context);
      if (sendErrors) {
        storeObjectHierarchy(root, context);
      }
      throwNonPortableException(root, reason, context,
                                "Attempt to share an instance of a non-portable class by assigning it to a root.");
    }
  }

  public void checkPortabilityOfField(Object fieldValue, String fieldName, Object pojo) throws TCNonPortableObjectError {
    NonPortableReason reason = checkPortabilityOf(fieldValue);
    if (reason != null) {
      NonPortableFieldSetContext context = this.appEventContextFactory.createNonPortableFieldSetContext(pojo,
                                                                                                        fieldName,
                                                                                                        fieldValue);
      dumpObjectHierarchy(fieldValue, context);
      if (sendErrors) {
        storeObjectHierarchy(pojo, context);
      }
      throwNonPortableException(pojo, reason, context,
                                "Attempt to set the field of a shared object to an instance of a non-portable class.");
    }
  }

  /**
   * This is used by the senders of ApplicationEvents to provide a version of a logically-managed pojo in the state it
   * would have been in had the ApplicationEvent not occurred.
   */
  public Object cloneAndInvokeLogicalOperation(Object pojo, String methodName, Object[] params) {
    try {
      Class c = pojo.getClass();
      Object o = c.newInstance();
      if (o instanceof Map) {
        ((Map) o).putAll((Map) pojo);
      } else if (o instanceof Collection) {
        ((Collection) o).addAll((Collection) pojo);
      }
      Method[] methods = c.getMethods();
      methodName = methodName.substring(0, methodName.indexOf('('));
      for (int i = 0; i < methods.length; i++) {
        Method m = methods[i];
        Class[] paramTypes = m.getParameterTypes();
        if (m.getName().equals(methodName) && params.length == paramTypes.length) {
          for (int j = 0; j < paramTypes.length; j++) {
            if (!paramTypes[j].isAssignableFrom(params[j].getClass())) {
              m = null;
              break;
            }
          }
          if (m != null) {
            m.invoke(o, params);
            break;
          }
        }
      }
      pojo = o;
    } catch (Exception e) {
      logger.error("Unable to clone logical object", e);
    }
    return pojo;
  }

  public void checkPortabilityOfLogicalAction(Object[] params, int index, String methodName, Object pojo)
      throws TCNonPortableObjectError {
    Object param = params[index];
    NonPortableReason reason = checkPortabilityOf(param);
    if (reason != null) {
      NonPortableEventContext context = this.appEventContextFactory
          .createNonPortableLogicalInvokeContext(pojo, methodName, params, index);
      dumpObjectHierarchy(params[index], context);
      if (sendErrors) {
        storeObjectHierarchy(cloneAndInvokeLogicalOperation(pojo, methodName, params), context);
      }
      throwNonPortableException(pojo, reason, context,
                                "Attempt to share an instance of a non-portable class by"
                                    + " passing it as an argument to a method of a logically-managed class.");
    }
  }

  private void throwNonPortableException(Object obj, NonPortableReason reason, NonPortableEventContext context,
                                         String message) throws TCNonPortableObjectError {
    // XXX: The message should probably be part of the context
    reason.setMessage(message);
    context.addDetailsTo(reason);

    // Send this event to L2
    JMXMessage jmxMsg = channel.getJMXMessage();
    jmxMsg.setJMXObject(new NonPortableObjectEvent(context, reason));
    jmxMsg.send();

    StringWriter formattedReason = new StringWriter();
    PrintWriter out = new PrintWriter(formattedReason);
    StringFormatter sf = new StringFormatter();

    ParagraphFormatter pf = new ConsoleParagraphFormatter(80, sf);
    NonPortableReasonFormatter reasonFormatter = new ConsoleNonPortableReasonFormatter(out, ": ", sf, pf);
    reason.accept(reasonFormatter);
    reasonFormatter.flush();

    throw new TCNonPortableObjectError(formattedReason.getBuffer().toString());
  }

  private NonPortableReason checkPortabilityOf(Object obj) {
    if (!isPortableInstance(obj)) { return portability.getNonPortableReason(obj.getClass()); }
    return null;
  }

  private boolean rootLookupInProgress(String rootName) {
    return rootLookupsInProgress.contains(rootName);
  }

  private void markRootLookupInProgress(String rootName) {
    boolean wasAdded = rootLookupsInProgress.add(rootName);
    if (!wasAdded) throw new AssertionError("Attempt to mark a root lookup that is already in progress.");
  }

  private void markRootLookupNotInProgress(String rootName) {
    boolean removed = rootLookupsInProgress.remove(rootName);
    if (!removed) throw new AssertionError("Attempt to unmark a root lookup that wasn't in progress.");
  }

  public synchronized void replaceRootIDIfNecessary(String rootName, ObjectID newRootID) {
    waitUntilRunning();

    ObjectID oldRootID = (ObjectID) roots.get(rootName);
    if (oldRootID == null || oldRootID.equals(newRootID)) { return; }

    roots.put(rootName, newRootID);
  }

  private Object lookupRootOptionallyCreateOrReplace(String rootName, Object rootPojo, boolean create,
                                                     boolean dsoFinal, boolean noDepth) throws ClassNotFoundException {
    boolean replaceRootIfExistWhenCreate = !dsoFinal && create;

    ObjectID rootID = null;

    boolean retrieveNeeded = false;
    boolean isNew = false;
    boolean lookupInProgress = false;
    boolean isInterrupted = false;

    synchronized (this) {
      while (true) {
        if (!replaceRootIfExistWhenCreate) {
          rootID = (ObjectID) roots.get(rootName);
          if (rootID != null) {
            break;
          }
        } else {
          rootID = ObjectID.NULL_ID;
        }
        if (!rootLookupInProgress(rootName)) {
          lookupInProgress = true;
          markRootLookupInProgress(rootName);
          break;
        } else {
          try {
            wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
            isInterrupted = true;
          }
        }
      }
    }
    Util.selfInterruptIfNeeded(isInterrupted);

    retrieveNeeded = lookupInProgress && !replaceRootIfExistWhenCreate;

    isNew = retrieveNeeded || (rootID.isNull() && create);

    if (retrieveNeeded) {
      rootID = remoteObjectManager.retrieveRootID(rootName);
    }

    if (rootID.isNull() && create) {
      Assert.assertNotNull(rootPojo);
      // TODO:: Optimize this, do lazy instantiation
      TCObject root = null;
      if (isLiteralPojo(rootPojo)) {
        root = basicCreateIfNecessary(rootPojo);
      } else {
        root = lookupOrCreate(rootPojo, this.appEventContextFactory.createNonPortableRootContext(rootName, rootPojo));
      }
      rootID = root.getObjectID();
      txManager.createRoot(rootName, rootID);
    }

    synchronized (this) {
      if (isNew && !rootID.isNull()) roots.put(rootName, rootID);
      if (lookupInProgress) {
        markRootLookupNotInProgress(rootName);
        notifyAll();
      }
    }

    return lookupObject(rootID, null, noDepth);
  }

  private TCObject basicLookupByID(ObjectID id) {
    return (TCObject) idToManaged.get(id);
  }

  private boolean basicHasLocal(ObjectID id) {
    return basicLookupByID(id) != null;
  }

  private TCObject basicLookup(Object obj) {
    TCObject tcobj;
    if (obj instanceof Manageable) {
      tcobj = ((Manageable) obj).__tc_managed();
    } else {
      synchronized (pojoToManaged) {
        tcobj = (TCObject) pojoToManaged.get(obj);
      }
    }
    return tcobj;
  }

  private void basicAddLocal(TCObject obj) {
    synchronized (this) {
      Assert.eval(!(obj instanceof TCObjectClone));
      if (basicHasLocal(obj.getObjectID())) { throw Assert.failure("Attempt to add an object that already exists: "
                                                                   + obj); }
      idToManaged.put(obj.getObjectID(), obj);

      Object pojo = obj.getPeerObject();

      if (pojo != null) {
        if (pojo.getClass().isArray()) {
          ManagerUtil.register(pojo, obj);
        }

        synchronized (pojoToManaged) {
          if (pojo instanceof Manageable) {
            Manageable m = (Manageable) pojo;
            if (m.__tc_managed() == null) {
              m.__tc_managed(obj);
            } else {
              Assert.assertTrue(m.__tc_managed() == obj);
            }
          } else {
            if (!isLiteralPojo(pojo)) {
              pojoToManaged.put(obj.getPeerObject(), obj);
            }
          }
        }
      }
      cache.add(obj);
      notifyAll();
    }
  }

  private void addToManagedFromRoot(Object root, NonPortableEventContext context) {
    traverser.traverse(root, traverseTest, context);
  }

  private void dumpObjectHierarchy(Object root, NonPortableEventContext context) {
    // the catch is not in the called method so that when/if there is an OOME, the logging might have a chance of
    // actually working (as opposed to just throwing another OOME)
    try {
      dumpObjectHierarchy0(root, context);
    } catch (Throwable t) {
      logger.error("error walking non-portable object instance of type " + root.getClass().getName(), t);
    }
  }

  private void dumpObjectHierarchy0(Object root, NonPortableEventContext context) {
    if (runtimeLogger.getNonPortableDump()) {
      NonPortableWalkVisitor visitor = new NonPortableWalkVisitor(CustomerLogging.getDSORuntimeLogger(), this,
                                                                  this.clientConfiguration, root);
      ObjectGraphWalker walker = new ObjectGraphWalker(root, visitor, visitor);
      walker.walk();
    }
  }

  public void sendApplicationEvent(Object pojo, ApplicationEvent event) {
    JMXMessage jmxMsg = channel.getJMXMessage();
    storeObjectHierarchy(pojo, event.getApplicationEventContext());
    jmxMsg.setJMXObject(event);
    jmxMsg.send();
  }

  public void storeObjectHierarchy(Object root, ApplicationEventContext context) {
    try {
      WalkVisitor wv = new WalkVisitor(this, this.clientConfiguration, root, context);
      ObjectGraphWalker walker = new ObjectGraphWalker(context.getPojo(), wv, wv);
      walker.walk();
      context.setTreeModel(wv.getTreeModel());
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private void addToSharedFromRoot(Object root, NonPortableEventContext context) {
    shareObjectsTraverser.traverse(root, traverseTest, context);
  }

  public ToggleableStrongReference getOrCreateToggleRef(ObjectID id, Object peer) {
    return referenceManager.getOrCreateFor(id, peer);
  }

  private class AddManagedObjectAction implements TraversalAction {
    public void visit(List objects) {
      List tcObjects = basicCreateIfNecessary(objects);
      for (Iterator i = tcObjects.iterator(); i.hasNext();) {
        txManager.createObject((TCObject) i.next());
      }
    }
  }

  private class SharedObjectsAction implements TraversalAction {
    public void visit(List objects) {
      basicShareObjectsIfNecessary(objects);
    }
  }

  private class NewObjectTraverseTest implements TraverseTest {

    public boolean shouldTraverse(Object object) {
      // literals should be skipped -- without this check, literal members (field values, array element values, in
      // collection, etc) of newly shared instances would get TCObjects and ObjectIDs assigned to them.
      if (literals.isLiteralInstance(object)) { return false; }

      TCObject tco = basicLookup(object);
      if (tco == null) { return true; }
      return tco.isNew();
    }

    public void checkPortability(TraversedReference reference, Class referringClass, NonPortableEventContext context)
        throws TCNonPortableObjectError {
      ClientObjectManagerImpl.this.checkPortabilityOfTraversedReference(reference, referringClass, context);
    }
  }

  private TCObject basicCreateIfNecessary(Object pojo) {
    TCObject obj = null;

    if ((obj = basicLookup(pojo)) == null) {
      obj = factory.getNewInstance(nextObjectID(), pojo, pojo.getClass(), true);
      txManager.createObject(obj);
      basicAddLocal(obj);
      executePostCreateMethod(pojo);
    }
    return obj;
  }

  private synchronized List basicCreateIfNecessary(List pojos) {
    waitUntilRunning();
    List tcObjects = new ArrayList(pojos.size());
    for (Iterator i = pojos.iterator(); i.hasNext();) {
      tcObjects.add(basicCreateIfNecessary(i.next()));
    }
    return tcObjects;
  }

  private TCObject basicShareObjectIfNecessary(Object pojo) {
    TCObject obj = null;

    if ((obj = basicLookup(pojo)) == null) {
      obj = factory.getNewInstance(nextObjectID(), pojo, pojo.getClass(), true);
      pendingCreateTCObjects.add(obj);
      basicAddLocal(obj);
    }
    return obj;
  }

  private synchronized List basicShareObjectsIfNecessary(List pojos) {
    waitUntilRunning();
    List tcObjects = new ArrayList(pojos.size());
    for (Iterator i = pojos.iterator(); i.hasNext();) {
      tcObjects.add(basicShareObjectIfNecessary(i.next()));
    }
    return tcObjects;
  }

  public synchronized void addPendingCreateObjectsToTransaction() {
    for (Iterator i = pendingCreateTCObjects.iterator(); i.hasNext();) {
      TCObject tcObject = (TCObject) i.next();
      txManager.createObject(tcObject);
    }
    pendingCreateTCObjects.clear();
  }

  public synchronized boolean hasPendingCreateObjects() {
    return !pendingCreateTCObjects.isEmpty();
  }

  private ObjectID nextObjectID() {
    return idProvider.next();
  }

  public WeakReference createNewPeer(TCClass clazz, DNA dna) {
    if (clazz.isUseNonDefaultConstructor()) {
      try {
        return new WeakObjectReference(dna.getObjectID(), factory.getNewPeerObject(clazz, dna), referenceQueue);
      } catch (Exception e) {
        throw new TCRuntimeException(e);
      }
    } else {
      return createNewPeer(clazz, dna.getArraySize(), dna.getObjectID(), dna.getParentObjectID());
    }
  }

  /**
   * Deep Clone support
   */
  public Object createNewCopyInstance(Object source, Object parent) {
    Assert.eval(!isLiteralPojo(source));

    TCClass clazz = this.getOrCreateClass(source.getClass());

    try {
      if (clazz.isProxyClass()) {
        InvocationHandler srcHandler = Proxy.getInvocationHandler(source);
        Class peerClass = clazz.getPeerClass();
        return Proxy.newProxyInstance(peerClass.getClassLoader(), peerClass.getInterfaces(), srcHandler);
      } else if (clazz.isIndexed()) {
        int size = Array.getLength(source);
        return factory.getNewArrayInstance(clazz, size);
      } else if (clazz.isNonStaticInner()) {
        Assert.eval(parent != null);
        return factory.getNewPeerObject(clazz, parent);
      } else {
        Assert.eval(parent == null);
        Object o = factory.getNewPeerObject(clazz);
        return o;
      }
    } catch (Exception e) {
      throw new TCRuntimeException(e);
    }
  }

  public WeakReference createNewPeer(TCClass clazz, int size, ObjectID id, ObjectID parentID) {
    try {
      if (clazz.isIndexed()) {
        Object array = factory.getNewArrayInstance(clazz, size);
        return new WeakObjectReference(id, array, referenceQueue);
      } else if (parentID.isNull()) {
        return new WeakObjectReference(id, factory.getNewPeerObject(clazz), referenceQueue);
      } else {
        return new WeakObjectReference(id, factory.getNewPeerObject(clazz, lookupObject(parentID)), referenceQueue);
      }
    } catch (Exception e) {
      throw new TCRuntimeException(e);
    }
  }

  public TCClass getOrCreateClass(Class clazz) {
    return clazzFactory.getOrCreate(clazz, this);
  }

  public boolean isPortableClass(Class clazz) {
    return portability.isPortableClass(clazz);
  }

  public boolean isPortableInstance(Object obj) {
    return portability.isPortableInstance(obj);
  }

  private void startReaper() {
    reaper = new StoppableThread("Reaper") {
      public void run() {
        while (true) {
          try {
            if (isStopRequested()) { return; }

            WeakObjectReference wor = (WeakObjectReference) referenceQueue.remove(POLL_TIME);

            if (wor != null) {
              ObjectID objectID = wor.getObjectID();
              reap(objectID);
            }
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    };
    reaper.setDaemon(true);
    reaper.start();
  }

  // XXX::: Cache eviction doesnt clear it from the cache. it happens in reap().
  public void evictCache(CacheStats stat) {
    int size = idToManaged_size();
    int toEvict = stat.getObjectCountToEvict(size);
    if (toEvict <= 0) return;
    // Cache is full
    boolean debug = logger.isDebugEnabled();
    int totalReferencesCleared = 0;
    int toClear = toEvict;
    while (toEvict > 0 && toClear > 0) {
      int maxCount = Math.min(COMMIT_SIZE, toClear);
      Collection removalCandidates = cache.getRemovalCandidates(maxCount);
      if (removalCandidates.isEmpty()) break; // couldnt find any more
      for (Iterator i = removalCandidates.iterator(); i.hasNext() && toClear > 0;) {
        TCObject removed = (TCObject) i.next();
        if (removed != null) {
          Object pr = removed.getPeerObject();
          if (pr != null) {
            // We don't want to take dso locks while clearing since it will happen inside the scope of the resolve lock
            // (see CDV-596)
            txManager.disableTransactionLogging();
            final int cleared;
            try {
              cleared = removed.clearReferences(toClear);
            } finally {
              txManager.enableTransactionLogging();
            }

            totalReferencesCleared += cleared;
            if (debug) {
              logger.debug("Clearing:" + removed.getObjectID() + " class:" + pr.getClass() + " Total cleared =  "
                           + totalReferencesCleared);
            }
            toClear -= cleared;
          }
        }
      }
      toEvict -= removalCandidates.size();
    }
    // TODO:: Send the correct set of targetObjects2GC
    stat.objectEvicted(totalReferencesCleared, idToManaged_size(), Collections.EMPTY_LIST);
  }

  // XXX:: Not synchronizing to improve performance, should be called only during cache eviction
  private int idToManaged_size() {
    return idToManaged.size();
  }

  public String dump() {
    StringWriter writer = new StringWriter();
    PrintWriter pw = new PrintWriter(writer);
    new PrettyPrinterImpl(pw).visit(this);
    writer.flush();
    return writer.toString();
  }

  public void dump(Writer writer) {
    try {
      writer.write(dump());
      writer.flush();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  public void dumpToLogger() {
    logger.info(dump());
  }

  public synchronized PrettyPrinter prettyPrint(PrettyPrinter out) {
    out.println(getClass().getName());
    out.indent().print("roots Map: ").println(new Integer(roots.size()));
    out.indent().print("idToManaged size: ").println(new Integer(idToManaged.size()));
    out.indent().print("pojoToManaged size: ").println(new Integer(pojoToManaged.size()));
    return out;
  }

  private static class LocalLookupContext {
    private final ResetableLatch latch               = new ResetableLatch();
    private final Counter        callStackCount      = new Counter(0);
    private final Counter        objectCreationCount = new Counter(0);
    private final Set            objectLatchWaitSet  = new HashSet();

    public ResetableLatch getLatch() {
      return latch;
    }

    public Counter getCallStackCount() {
      return callStackCount;
    }

    public Counter getObjectCreationCount() {
      return objectCreationCount;
    }

    public Set getObjectLatchWaitSet() {
      return objectLatchWaitSet;
    }

  }

  private static class ObjectLatchState {

    private static final State   CREATE_STATE = new State("CREATE-STATE");

    private static final State   LOOKUP_STATE = new State("LOOKUP-STATE");

    private final ObjectID       objectID;

    private final ResetableLatch latch;

    private State                state        = LOOKUP_STATE;

    private TCObject             object;

    public ObjectLatchState(ObjectID objectID, ResetableLatch latch) {
      this.objectID = objectID;
      this.latch = latch;
    }

    public void setObject(TCObject obj) {
      this.object = obj;
    }

    public ObjectID getObjectID() {
      return objectID;
    }

    public ResetableLatch getLatch() {
      return latch;
    }

    public TCObject getObject() {
      return object;
    }

    public boolean isLookupState() {
      return LOOKUP_STATE.equals(state);
    }

    public boolean isCreateState() {
      return CREATE_STATE.equals(state);
    }

    public void markCreateState() {
      state = CREATE_STATE;
    }

    public String toString() {
      return "ObjectLatchState [" + objectID + " , " + latch + ", " + state + " ]";
    }
  }

}