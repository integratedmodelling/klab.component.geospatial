package org.integratedmodelling.geospatial.adapters.raster;

import java.util.Arrays;
import java.util.EnumSet;

public class BandMixing {
  public enum Operation {
    MAX_VALUE("max_value"),
    MIN_VALUE("min_value"),
    AVG_VALUE("avg_value"),
    SUM_VALUE("sum_value"),
    BAND_MAX_VALUE("band_max_value"),
    BAND_MIN_VALUE("band_min_value");

    private final String label;

    private Operation(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }
  }

  public static EnumSet<Operation> implementedOperations =
      EnumSet.of(
          Operation.MAX_VALUE,
          Operation.MIN_VALUE,
          Operation.AVG_VALUE,
          Operation.SUM_VALUE,
          Operation.BAND_MAX_VALUE,
          Operation.BAND_MIN_VALUE);
}
