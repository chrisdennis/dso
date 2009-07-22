/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.tc.admin;

import org.terracotta.modules.configuration.Presentation;
import org.terracotta.modules.configuration.PresentationContext;
import org.terracotta.modules.configuration.PresentationFactory;

import com.tc.admin.common.XContainer;
import com.tc.admin.model.IClusterModel;

import java.awt.BorderLayout;

import javax.swing.BorderFactory;

public class FeaturePanel extends XContainer {
  protected Feature             feature;
  protected IAdminClientContext adminClientContext;
  protected IClusterModel       clusterModel;
  protected Presentation        presentation;

  public FeaturePanel(Feature feature, IAdminClientContext adminClientContext, IClusterModel clusterModel) {
    super(new BorderLayout());

    this.feature = feature;
    this.adminClientContext = adminClientContext;
    this.clusterModel = clusterModel;

    XContainer panel = new XContainer();
    panel.setBorder(BorderFactory.createTitledBorder(feature.getDisplayName()));
    add(panel);

    try {
      PresentationFactory factory;
      if ((factory = feature.getPresentationFactory()) != null) {
        presentation = factory.create(PresentationContext.DEV);
        if (presentation != null) {
          add(presentation);
          presentation.setup(adminClientContext, clusterModel);
        } else {
          System.err.println("Failed to instantiate instance of '" + factory + "'");
        }
      } else {
        System.err.println("Failed to load PresentationFactory for feature '" + feature + "'");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void tearDown() {
    if (presentation != null) {
      presentation.tearDown();
    }

    synchronized (this) {
      feature = null;
      adminClientContext = null;
      clusterModel = null;
      presentation = null;
    }

    super.tearDown();
  }
}
