/*
 * All content copyright (c) 2003-2008 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.object.applicator;

import com.tc.object.ClientObjectManager;
import com.tc.object.SerializationUtil;
import com.tc.object.TCObject;
import com.tc.object.TraversedReferences;
import com.tc.object.dna.api.DNA;
import com.tc.object.dna.api.DNACursor;
import com.tc.object.dna.api.DNAEncoding;
import com.tc.object.dna.api.DNAWriter;
import com.tc.object.dna.api.LogicalAction;
import com.tc.util.Assert;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * ChangeApplicator for Dates.
 */
public class DateApplicator extends BaseApplicator {

  public DateApplicator(DNAEncoding encoding) {
    super(encoding);
  }

  public void hydrate(ClientObjectManager objectManager, TCObject tcObject, DNA dna, Object pojo) throws IOException,
      ClassNotFoundException {
    Date date = (Date) pojo;
    DNACursor cursor = dna.getCursor();

    while (cursor.next(encoding)) {
      LogicalAction action = cursor.getLogicalAction();
      int method = action.getMethod();
      Object[] params = action.getParameters();
      switch (method) {
        case SerializationUtil.SET_TIME:
          Assert.assertNotNull(params[0]);
          date.setTime(((Long) params[0]).longValue());
          break;
        case SerializationUtil.SET_NANOS:
          if (date instanceof Timestamp) {
            Assert.assertNotNull(params[0]);
            ((Timestamp) date).setNanos(((Integer) params[0]).intValue());
          }
          break;
        default:
          throw new AssertionError("invalid action:" + method);
      }
    }
  }

  public void dehydrate(ClientObjectManager objectManager, TCObject tcObject, DNAWriter writer, Object pojo) {
    Date date = (Date) pojo;
    writer.addLogicalAction(SerializationUtil.SET_TIME, new Object[] { new Long(date.getTime()) });
  }

  public TraversedReferences getPortableObjects(Object pojo, TraversedReferences addTo) {
    addTo.addAnonymousReference(pojo);
    return addTo;
  }

  public Object getNewInstance(ClientObjectManager objectManager, DNA dna) {
    throw new UnsupportedOperationException();
  }

}
