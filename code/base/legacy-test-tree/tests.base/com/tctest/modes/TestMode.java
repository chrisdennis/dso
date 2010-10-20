/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.tctest.modes;

import com.tc.test.TestConfigObject;

public interface TestMode {
  public enum Mode {
    NORMAL, CRASH, ACTIVE_PASSIVE, ACTIVE_ACTIVE;

    public static Mode fromString(String mode) {
      if (TestConfigObject.TRANSPARENT_TESTS_MODE_NORMAL.equals(mode)) {
        return NORMAL;
      } else if (TestConfigObject.TRANSPARENT_TESTS_MODE_CRASH.equals(mode)) {
        return CRASH;
      } else if (TestConfigObject.TRANSPARENT_TESTS_MODE_ACTIVE_PASSIVE.equals(mode)) {
        return ACTIVE_PASSIVE;
      } else if (TestConfigObject.TRANSPARENT_TESTS_MODE_ACTIVE_ACTIVE.equals(mode)) { return ACTIVE_ACTIVE; }

      throw new AssertionError(mode + " Not Supported");
    }

    @Override
    public String toString() {
      switch (this) {
        case NORMAL:
          return TestConfigObject.TRANSPARENT_TESTS_MODE_NORMAL;
        case CRASH:
          return TestConfigObject.TRANSPARENT_TESTS_MODE_CRASH;
        case ACTIVE_PASSIVE:
          return TestConfigObject.TRANSPARENT_TESTS_MODE_ACTIVE_PASSIVE;
        case ACTIVE_ACTIVE:
          return TestConfigObject.TRANSPARENT_TESTS_MODE_ACTIVE_ACTIVE;
      }

      throw new AssertionError();
    }
  }

  Mode getMode();

  TestSetupManager getSetupManager();
}
