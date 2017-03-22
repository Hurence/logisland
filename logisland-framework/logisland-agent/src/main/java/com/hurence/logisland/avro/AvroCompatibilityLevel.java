
package com.hurence.logisland.avro;

public enum AvroCompatibilityLevel {
  NONE("NONE", AvroCompatibilityChecker.NO_OP_CHECKER),
  BACKWARD("BACKWARD", AvroCompatibilityChecker.BACKWARD_CHECKER),
  FORWARD("FORWARD", AvroCompatibilityChecker.FORWARD_CHECKER),
  FULL("FULL", AvroCompatibilityChecker.FULL_CHECKER);

  public final String name;
  public final AvroCompatibilityChecker compatibilityChecker;

  private AvroCompatibilityLevel(String name, AvroCompatibilityChecker compatibilityChecker) {
    this.name = name;
    this.compatibilityChecker = compatibilityChecker;
  }

  public static AvroCompatibilityLevel forName(String name) {
    if (name == null) {
      return null;
    }

    name = name.toUpperCase();
    if (NONE.name.equals(name)) {
      return NONE;
    } else if (BACKWARD.name.equals(name)) {
      return BACKWARD;
    } else if (FORWARD.name.equals(name)) {
      return FORWARD;
    } else if (FULL.name.equals(name)) {
      return FULL;
    } else {
      return null;
    }
  }
}
