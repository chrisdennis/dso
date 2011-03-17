/*
 * All content copyright (c) 2003-2008 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.net;

import java.util.Arrays;
import java.util.TreeSet;

/**
 * This class's purpose is to give a definite order to a set of GroupIDs and determine the coordinator GroupID, that way
 * you don't have to encode the GroupIDs mapping in many places.
 */
public class OrderedGroupIDs {

  private final GroupID[] groupIDs;

  public OrderedGroupIDs(GroupID[] gids) {
    TreeSet<GroupID> sorted = new TreeSet<GroupID>(Arrays.asList(gids));
    this.groupIDs = sorted.toArray(new GroupID[sorted.size()]);

  }

  public GroupID[] getGroupIDs() {
    return this.groupIDs.clone();
  }

  public int length() {
    return this.groupIDs.length;
  }

  public GroupID getGroup(int i) {
    return this.groupIDs[i];
  }

  public int getGroupIDIndex(GroupID gid) {
    return Arrays.binarySearch(this.groupIDs, gid);
  }

  public GroupID getActiveCoordinatorGroup() {
    // This assumption that index 0 is coordinator group should not be exposed anywhere else.
    return this.groupIDs[0];
  }

}
