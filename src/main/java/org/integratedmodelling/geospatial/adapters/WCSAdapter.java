package org.integratedmodelling.geospatial.adapters;

import org.geotools.coverage.grid.GridCoverage2D;
import org.hortonmachine.gears.io.rasterreader.OmsRasterReader;
import org.hortonmachine.gears.io.rasterwriter.OmsRasterWriter;
import org.hortonmachine.gears.libs.modules.HMRaster;
import org.hortonmachine.gears.utils.RegionMap;
import org.integratedmodelling.geospatial.adapters.wcs.WCSServiceManager;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.exceptions.KlabIOException;
import org.integratedmodelling.klab.api.exceptions.KlabUnimplementedException;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.geometry.impl.GeometryImpl;
import org.integratedmodelling.klab.api.knowledge.Artifact;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.Urn;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Projection;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.resources.adapters.ResourceAdapter;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.runtime.scale.space.ProjectionImpl;
import org.integratedmodelling.klab.utilities.Utils;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles "klab:random:...." URNs. Produces various types of random data, objects, or events. The
 * namespace (third field of the URN) selects the type of object:
 *
 * <p>WCS is service-bound so it's embeddable.
 *
 * @author Ferd
 */
@ResourceAdapter(
    name = "wcs",
    version = Version.CURRENT,
    type = Artifact.Type.NUMBER,
    embeddable = true)
public class WCSAdapter {

  static Map<String, WCSServiceManager> services = new HashMap<>();

  public WCSAdapter() {}

  /**
   * Get the service handler for the passed service URL and version.
   *
   * @param serviceUrl
   * @param version
   * @return a WCS service. Inspect for errors before using.
   */
  public static WCSServiceManager getService(String serviceUrl, Version version) {

    var key = serviceUrl + ":" + version;

    if (services.containsKey(key)) {
      return services.get(key);
    }
    WCSServiceManager ret = new WCSServiceManager(serviceUrl, version);
    if (ret != null) {
      services.put(key, ret);
    }
    return ret;
  }

  @ResourceAdapter.Encoder
  public void encode(
      Resource resource,
      Urn urn,
      Data.Builder builder,
      Geometry geometry,
      Observable observable,
      Scope scope) {

    //    String interpolation =
    //        urn.getParameters()
    //            .get("interpolation", resource.getParameters().get("interpolation",
    // String.class));

    WCSServiceManager service =
        getService(
            resource.getParameters().get("serviceUrl", String.class),
            Version.create(resource.getParameters().get("wcsVersion", String.class)));
    var layer = service.getLayer(resource.getParameters().get("wcsIdentifier", String.class));
    if (layer != null) {
      //      encoder.encodeFromCoverage(
      //          resource,
      //          urnParameters,
      //          getCoverage(layer, resource, geometry, interpolation),
      //          geometry,
      //          builder,
      //          scope);
    } else {
      builder.notification(
          Notification.error(
              "Problems accessing WCS layer "
                  + resource.getParameters().get("wcsIdentifier", String.class)));
    }
  }

  @ResourceAdapter.Validator(phase = ResourceAdapter.Validator.LifecyclePhase.LocalImport)
  public boolean validateImported(Resource resource) {

    return false;
  }

  public static File getAdjustedCoverage(String url, Geometry geometry) {
    try (InputStream input = new URL(url).openStream()) {
      URL getCov = new URL(url);
      return getAdjustedCoverage(getCov.openStream(), geometry);
    } catch (Throwable e) {
      throw new KlabIOException(e);
    }
  }

  public static File getAdjustedCoverage(InputStream input, Geometry geometry) {

    try {

      File coverageFile = File.createTempFile("geo", ".tiff");
      Files.copy(input, coverageFile.toPath());

      var space = geometry.dimension(Geometry.Dimension.Type.SPACE);
      String rcrs =
          space.getParameters().get(GeometryImpl.PARAMETER_SPACE_PROJECTION, String.class);
      var crs = Projection.of(rcrs);
      int cols = space.getShape().get(0).intValue();
      int rows = space.getShape().get(1).intValue();
      double[] extent =
          space.getParameters().get(GeometryImpl.PARAMETER_SPACE_BOUNDINGBOX, double[].class);

      GridCoverage2D coverage = OmsRasterReader.readRaster(coverageFile.getAbsolutePath());
      var envelope = coverage.getEnvelope();
      var lowerCorner = envelope.getLowerCorner();
      double[] westSouth = lowerCorner.getCoordinate();
      var upperCorner = envelope.getUpperCorner();
      double[] eastNorth = upperCorner.getCoordinate();

      org.locationtech.jts.geom.Envelope requestedExtend =
          new org.locationtech.jts.geom.Envelope(extent[0], extent[1], extent[2], extent[3]);
      org.locationtech.jts.geom.Envelope recievedExtend =
          new org.locationtech.jts.geom.Envelope(
              westSouth[0], eastNorth[0], westSouth[1], eastNorth[1]);

      double receivedArea = recievedExtend.getArea();
      double requestedArea = requestedExtend.getArea();
      double diff = Math.abs(requestedArea - receivedArea);
      if (diff > 0.01 && crs instanceof ProjectionImpl projection) {
        // need to pad
        var raster = HMRaster.fromGridCoverage(coverage);
        var region = RegionMap.fromEnvelopeAndGrid(requestedExtend, cols, rows);
        var paddedRaster =
            new HMRaster.HMRasterWritableBuilder()
                .setName("padded")
                .setRegion(region)
                .setCrs(projection.getCRS())
                .setNoValue(raster.getNovalue())
                .build();
        paddedRaster.mapRaster(null, raster, null);
        coverage = paddedRaster.buildCoverage();
        OmsRasterWriter.writeRaster(coverageFile.getAbsolutePath(), coverage);
      }

      coverageFile.deleteOnExit();
//      if (Configuration.INSTANCE.isEchoEnabled()) {
//        System.out.println("Data have arrived in " + coverageFile);
//      }

      return coverageFile;

    } catch (Throwable e) {
      throw new KlabIOException(e);
    }
  }
}
