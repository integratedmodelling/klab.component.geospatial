package org.integratedmodelling.geospatial.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.awt.Point;
import java.awt.image.*;
import java.io.*;
import java.util.List;
import javax.imageio.ImageIO;
import org.integratedmodelling.klab.api.data.mediation.classification.DataKey;
import org.integratedmodelling.klab.api.knowledge.observation.impl.ObservationImpl;
import org.integratedmodelling.klab.api.lang.Annotation;
import org.junit.jupiter.api.Test;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.integratedmodelling.geospatial.library.GeodataIO;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.data.Storage;
import org.integratedmodelling.klab.api.scope.ContextScope;

class RasterRendererTest {
  @Test void exporterUsesObservationRampAndDisposesCoverage() throws Exception {
    var observation = observation("colors", List.of("black", "white"));
    var scanner = mock(Storage.DoubleScanner.class);
    var scope = mock(ContextScope.class);
    var coverage = mock(GridCoverage2D.class);
    var rendered = mock(RenderedImage.class);
    when(rendered.getData()).thenReturn(raster(0, 10));
    when(coverage.getRenderedImage()).thenReturn(rendered);
    when(coverage.getEnvelope2D()).thenReturn(new ReferencedEnvelope(0, 2, 0, 1, null));
    try (var geotools = mockStatic(Geotools.class)) {
      geotools.when(() -> Geotools.stateToCoverage(observation, scanner,
          DataBuffer.TYPE_FLOAT, Float.NaN, scope, false)).thenReturn(coverage);
      try (var stream = new GeodataIO().exportPNGContinuous(observation, scanner,
          Parameters.create("viewportX", 2.0, "viewportY", 1.0), scope)) {
        var image = ImageIO.read(stream);
        assertEquals(0xff000000, image.getRGB(0, 0));
        assertEquals(0xffffffff, image.getRGB(1, 0));
      }
      verify(coverage).dispose(true);
    }
  }
  private Raster raster(double... values) {
    return Raster.createWritableRaster(new BandedSampleModel(DataBuffer.TYPE_DOUBLE, values.length, 1, 1),
        new DataBufferDouble(values, values.length), new Point(3, 4));
  }
  private ObservationImpl observation(Object... args) {
    var ret = new ObservationImpl();
    ret.getAnnotations().add(Annotation.of("colormap", args));
    return ret;
  }
  @Test void centeredRasterSurvivesPngEncodingWithTransparency() throws Exception {
    var image = RasterRenderer.render(raster(-2, 0, 8, Double.NaN), 4,
        observation("colors", List.of("red", "white", "green"), "center", 0, "min", -2, "max", 8), null, 4, 1);
    var bytes = new ByteArrayOutputStream();
    assertTrue(ImageIO.write(image, "png", bytes));
    var decoded = ImageIO.read(new ByteArrayInputStream(bytes.toByteArray()));
    assertEquals(0xffff0000, decoded.getRGB(0, 0));
    assertEquals(0xffffffff, decoded.getRGB(1, 0));
    assertEquals(0xff008000, decoded.getRGB(2, 0));
    assertEquals(0, decoded.getRGB(3, 0));
  }
  @Test void keyedRasterResolvesValuesAndDistinguishesUnknownFromNodata() {
    var key = mock(DataKey.class);
    when(key.lookup(1)).thenReturn("land:Forest");
    var image = RasterRenderer.render(raster(1, 2, Double.NaN), 3,
        observation("values", List.of(List.of("land:Forest", "#228b22")), "unknown", "gray"), key, 3, 1);
    assertEquals(0xff228b22, image.getRGB(0, 0));
    assertEquals(0xff808080, image.getRGB(1, 0));
    assertEquals(0, image.getRGB(2, 0));
  }
  @Test void defaultRampUsesFiniteRangeAndPreservesAspect() {
    var image = RasterRenderer.render(raster(0, 10), 2, new ObservationImpl(), null, 20, 20);
    assertEquals(20, image.getWidth());
    assertEquals(10, image.getHeight());
    assertEquals(0xff440154, image.getRGB(0, 0));
    assertEquals(0xfffde725, image.getRGB(19, 0));
  }
}
