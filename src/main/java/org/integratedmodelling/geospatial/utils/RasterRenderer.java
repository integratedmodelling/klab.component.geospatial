package org.integratedmodelling.geospatial.utils;

import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import org.geotools.coverage.grid.GridCoverage2D;
import org.integratedmodelling.klab.api.data.mediation.classification.DataKey;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;
import org.integratedmodelling.klab.api.view.modeler.visualization.ColorRamp;

/** Raster visualization using the effective @colormap on the observation. */
public final class RasterRenderer {
  private RasterRenderer() {}

  public static BufferedImage render(
      GridCoverage2D coverage,
      Observation observation,
      DataKey key,
      int viewportWidth,
      int viewportHeight) {
    var envelope = coverage.getEnvelope2D();
    return render(
        coverage.getRenderedImage().getData(),
        envelope.getWidth() / envelope.getHeight(),
        observation,
        key,
        viewportWidth,
        viewportHeight);
  }

  /**
   * Samples the selected single-band raster using nearest-neighbor resampling. Category codes are
   * translated through the DataKey before lookup; they are never treated as concept URNs.
   */
  public static BufferedImage render(
      Raster raster,
      double aspect,
      Observation observation,
      DataKey key,
      int viewportWidth,
      int viewportHeight) {
    if (viewportWidth < 1
        || viewportHeight < 1
        || viewportWidth > 16384
        || viewportHeight > 16384
        || !Double.isFinite(aspect)
        || aspect <= 0) {
      throw new IllegalArgumentException("Invalid raster viewport or aspect ratio");
    }
    int width = viewportWidth,
        height = Math.max(1, (int) Math.min(Integer.MAX_VALUE, Math.round(width / aspect)));
    if (height > viewportHeight) {
      height = viewportHeight;
      width = Math.max(1, (int) Math.round(height * aspect));
    }
    if ((long) width * height > 64_000_000)
      throw new IllegalArgumentException("Raster viewport exceeds 64 million pixels");
    var ramp = ColorRamp.fromObservation(observation);
    double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
    if (!ramp.isCategorical()) {
      if (key != null)
        throw new IllegalArgumentException("Keyed observations require @colormap(values=...)");
      for (int y = 0; y < raster.getHeight(); y++) {
        for (int x = 0; x < raster.getWidth(); x++) {
          double value = raster.getSampleDouble(x + raster.getMinX(), y + raster.getMinY(), 0);
          if (Double.isFinite(value)) {
            min = Math.min(min, value);
            max = Math.max(max, value);
          }
        }
      }
    }
    var image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    for (int y = 0; y < height; y++) {
      int sourceY =
          raster.getMinY()
              + Math.min(raster.getHeight() - 1, (int) ((y + 0.5) * raster.getHeight() / height));
      for (int x = 0; x < width; x++) {
        int sourceX =
            raster.getMinX()
                + Math.min(raster.getWidth() - 1, (int) ((x + 0.5) * raster.getWidth() / width));
        double sample = raster.getSampleDouble(sourceX, sourceY, 0);
        Object value = sample;
        if (key != null && Double.isFinite(sample)) {
          value =
              sample == Math.rint(sample) && sample >= 0 && sample <= Integer.MAX_VALUE
                  ? key.lookup((int) sample)
                  : null;
          if (value == null) {
            image.setRGB(x, y, ramp.unknownArgb());
            continue;
          }
        }
        image.setRGB(x, y, ramp.argb(value, min, max));
      }
    }
    return image;
  }
}
