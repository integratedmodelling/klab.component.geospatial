package org.integratedmodelling.geospatial.adapters.raster;

import java.util.EnumSet;

public class BandMixing {
    public enum Operation {
        MAX_VALUE,
        MIN_VALUE,
        AVG_VALUE,
        SUM_VALUE,
        BAND_MAX_VALUE,
        BAND_MIN_VALUE,
    }

    EnumSet<Operation> implementedOperations = EnumSet.of(
            Operation.MAX_VALUE,
            Operation.MIN_VALUE,
            Operation.AVG_VALUE,
            Operation.SUM_VALUE,
            Operation.BAND_MAX_VALUE,
            Operation.BAND_MIN_VALUE);

}
