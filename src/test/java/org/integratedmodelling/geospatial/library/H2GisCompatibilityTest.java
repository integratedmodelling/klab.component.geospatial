package org.integratedmodelling.geospatial.library;

import static org.junit.jupiter.api.Assertions.*;
import java.sql.DriverManager;
import org.h2gis.functions.factory.H2GISFunctions;
import org.junit.jupiter.api.Test;

/** Actual extension initialization, not just successful dependency resolution. */
class H2GisCompatibilityTest {
  @Test void spatialExtensionLoadsAndComputesIndependentPlanarRectangleArea() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:h2:mem:hm012_compatibility")) {
      assertTrue(connection.getMetaData().getDatabaseProductVersion().startsWith("1.4.197"));
      H2GISFunctions.load(connection);
      try (var statement = connection.createStatement();
          var rows = statement.executeQuery("SELECT ST_AREA(ST_GEOMFROMTEXT('POLYGON((0 0,4 0,4 3,0 3,0 0))'))")) {
        assertTrue(rows.next());
        assertEquals(12.0, rows.getDouble(1), 0.0); // 4 * 3, coordinate-units squared.
        assertFalse(rows.wasNull());
        assertFalse(rows.next());
      }
    }
  }
}
