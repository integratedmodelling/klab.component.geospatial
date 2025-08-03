package org.integratedmodelling.geospatial.adapters;

import com.google.common.cache.*;
import org.geotools.coverage.grid.GridCoverage2D;
import org.hortonmachine.gears.io.rasterreader.OmsRasterReader;
import org.hortonmachine.gears.io.rasterwriter.OmsRasterWriter;
import org.hortonmachine.gears.libs.modules.HMRaster;
import org.hortonmachine.gears.utils.RegionMap;
import org.integratedmodelling.common.knowledge.GeometryRepository;
import org.integratedmodelling.common.logging.Logging;
import org.integratedmodelling.geospatial.adapters.raster.RasterEncoder;
import org.integratedmodelling.geospatial.adapters.wcs.WCSServiceManager;
import org.integratedmodelling.klab.api.collections.Pair;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.collections.Triple;
import org.integratedmodelling.klab.api.configuration.Configuration;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.exceptions.KlabIOException;
import org.integratedmodelling.klab.api.exceptions.KlabInternalErrorException;
import org.integratedmodelling.klab.api.exceptions.KlabUnimplementedException;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.geometry.impl.GeometryImpl;
import org.integratedmodelling.klab.api.knowledge.Artifact;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.Urn;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Projection;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.resources.adapters.Parameter;
import org.integratedmodelling.klab.api.services.resources.adapters.ResourceAdapter;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.configuration.ServiceConfiguration;
import org.integratedmodelling.klab.runtime.scale.space.ProjectionImpl;
import org.integratedmodelling.klab.utilities.Utils;
import org.opengis.coverage.grid.GridCoverage;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WCS layer adapter. Keeps a size-bound cache of downloaded TIFFs to optimize transfers. NOTE: the
 * cache only works if the geometry asked for is the same object, so it should be instantiated using
 * {@link GeometryRepository} and not directly.
 *
 * <p>WCS is service-bound so it's embeddable.
 *
 * @author Ferd
 */
@ResourceAdapter(
    name = "wcs",
    version = Version.CURRENT,
    type = Artifact.Type.NUMBER,
    embeddable = true,
    parameters = {
      @Parameter(
          name = "serviceUrl",
          type = Artifact.Type.URL,
          description = "Base URL of the WCS service"),
      @Parameter(
          name = "wcsIdentifier",
          type = Artifact.Type.TEXT,
          description = "Identifier of the WCS resource, including namespace when applicable"),
      @Parameter(
          name = "wcsVersion",
          type = Artifact.Type.TEXT,
          description = "The WCS version to use when connecting to the resource"),
      @Parameter(
          name = "transform",
          type = Artifact.Type.TEXT,
          description = "An optional expression to transform the result values",
          urnParameter = true,
          optional = true),
      @Parameter(
          name = "nodata",
          type = Artifact.Type.NUMBER,
          description =
              "The value used as no-data throughout the raster. Overrides any specification in the original file",
          urnParameter = true,
          optional = true),
      @Parameter(
          name = "band",
          type = Artifact.Type.NUMBER,
          urnParameter = true,
          description = "The band to extract from the result coverage. Default is band 0",
          optional = true),
      @Parameter(
          name = "bandMixer",
          type = Artifact.Type.TEXT,
          description =
              "Retrieve all bands and mix them to obtain the result based on this expression",
          optional = true),
      @Parameter(
          name = "interpolation",
          type = Artifact.Type.ENUM,
          urnParameter = true,
          enumValues = {"bicubic", "bilinear", "bicubic2", "nearest-neighbor"},
          description = "An optional interpolation type to use when rescaling",
          optional = true)
    })
public class WCSAdapter {

  static Map<String, WCSServiceManager> services = new HashMap<>();
  private final File adapterCacheDirectory;
  private final Map<String, File> filenames = new HashMap<>();

  private long maxCachedMBytes = 100;
  private final LoadingCache<
          Triple<WCSServiceManager.WCSLayer, Geometry, RasterAdapter.Interpolation>, File>
      fileCache;
  private AtomicInteger cacheHits = new AtomicInteger();

  public WCSAdapter() {
    this.adapterCacheDirectory = Configuration.INSTANCE.getTemporaryDataPath();
    // TODO read adapter properties (API to come) and reset maxCachedMBytes and cache dir if needed

    this.fileCache =
        CacheBuilder.newBuilder()
            .maximumWeight(maxCachedMBytes)
            .concurrencyLevel(1)
            .weigher(
                (Weigher<
                        Triple<WCSServiceManager.WCSLayer, Geometry, RasterAdapter.Interpolation>,
                        File>)
                    (key, value) -> (int) (value.length() / 1048576L))
            .removalListener(
                notification -> {
                  Utils.Files.deleteQuietly(notification.getValue());
                })
            .build(
                new CacheLoader<>() {
                  @Override
                  public File load(
                      Triple<WCSServiceManager.WCSLayer, Geometry, RasterAdapter.Interpolation> key)
                      throws Exception {

                    // forcing v1.0.0 for now, while I figure out the pain of WCS requests
                    URL getCov =
                        key.getFirst()
                            .buildRetrieveUrl(
                                Version.create("1.0.0"), key.getSecond(), key.getThird());

                    File ret = null;
                    try (InputStream input = getCov.openStream()) {
                      ret = getAdjustedCoverage(input, key.getSecond());
                    } catch (Throwable e) {
                      throw new KlabIOException(e);
                    }
                    return ret;
                  }
                });
  }

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

    Logging.INSTANCE.info(
        "Attempting to connect to WCS service at " + serviceUrl + " version " + version + " ...");
    WCSServiceManager ret = new WCSServiceManager(serviceUrl, version);
    if (!ret.hasErrors()) {
      Logging.INSTANCE.info(
          "Connected to WCS service at " + serviceUrl + " version " + version + ".");
      services.put(key, ret);
    } else {
      Logging.INSTANCE.info(
          "Unable to connect to WCS service at " + serviceUrl + " version " + version + ".");
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

    WCSServiceManager service =
        getService(
            resource.getParameters().get("serviceUrl", String.class),
            Version.create(resource.getParameters().get("wcsVersion", String.class)));
    var layer = service.getLayer(resource.getParameters().get("wcsIdentifier", String.class));

    if (layer != null) {
      var parameters = Utils.Resources.overrideParameters(resource, urn);
      var coverage = getCoverage(layer, service, observable, parameters, geometry);
      if (coverage == null) {
        builder.notification(
            Notification.error(
                "Cannot build coverage for WCS layer "
                    + resource.getParameters().get("wcsIdentifier", String.class)));
      } else {
        RasterEncoder.INSTANCE.encodeFromCoverage(
            resource, parameters, coverage, geometry, builder, observable, scope);
      }
    } else {
      builder.notification(
          Notification.error(
              "Problems accessing WCS layer "
                  + resource.getParameters().get("wcsIdentifier", String.class)));
    }
  }

  @ResourceAdapter.Validator(phase = ResourceAdapter.Validator.LifecyclePhase.LocalImport)
  public Notification validateImported(Resource resource) {
    try {
      var serviceUrl = resource.getParameters().get("serviceUrl", String.class);
      var version = Version.create(resource.getParameters().get("wcsVersion", String.class));
      WCSServiceManager service = getService(serviceUrl, version);
      if (service == null) {
        return Notification.warning("Unable to connect to WCS service at " + serviceUrl);
      }
      var layer = service.getLayer(resource.getParameters().get("wcsIdentifier", String.class));
      if (layer == null) {
        return Notification.warning(
            "Unable to find WCS layer "
                + resource.getParameters().get("wcsIdentifier", String.class)
                + " out of "
                + service.getLayers().size());
      }
      return Notification.info(
          "WCS layer " + layer.getName() + " found in service.", Notification.Outcome.Success);
    } catch (Throwable e) {
      return Notification.error(
          "Import caused an exception: " + e.getMessage(), e, Notification.Outcome.Failure);
    }
  }

  private GridCoverage getCoverage(
      WCSServiceManager.WCSLayer layer,
      WCSServiceManager service,
      Observable observable,
      Parameters<String> parameters,
      Geometry geometry) {

    var interpolation = RasterAdapter.Interpolation.getDefaultForType(observable);
    if (parameters.containsKey(RasterAdapter.INTERPOLATION_PARAM)) {
      interpolation =
          RasterAdapter.Interpolation.fromField(
              parameters.get(RasterAdapter.INTERPOLATION_PARAM, String.class));
    }
    return RasterEncoder.INSTANCE.readCoverage(getCachedFile(layer, geometry, interpolation));
  }

  private File getCachedFile(
      WCSServiceManager.WCSLayer layer,
      Geometry geometry,
      RasterAdapter.Interpolation interpolation) {
    var key = layer.getIdentifier() + "__" + geometry.key();
    try {
      return fileCache.get(Triple.of(layer, geometry, interpolation));
    } catch (ExecutionException e) {
      // shouldn't happen
      throw new KlabInternalErrorException(e);
    }
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
      Files.copy(input, coverageFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

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

      return coverageFile;

    } catch (Throwable e) {
      throw new KlabIOException(e);
    }
  }

  public static void main(String[] args) {

    String centralColombia =
        "τ0(1){ttype=LOGICAL,period=[1609459200000 1640995200000],tscope=1.0,"
            + "tunit=YEAR}S2(934,631){bbox=[-75.2281407807369 -72.67107290964314 3.5641500380320963 5"
            + ".302943221927137],"
            + "shape"
            + "=00000000030000000100000005C0522AF2DBCA0987400C8361185B1480C052CE99DBCA0987400C8361185B1480C052CE99DBCA098740153636BF7AE340C0522AF2DBCA098740153636BF7AE340C0522AF2DBCA0987400C8361185B1480,proj=EPSG:4326}";

    ServiceConfiguration.injectInstantiators();

    // TODO check this strange behavior
    //    var centralColombiaGeometry = GeometryRepository.INSTANCE.get(centralColombia,
    // Geometry.class);
    //    System.out.println(
    //        Geometry.create(centralColombia).key()
    //            + " >>>> "
    //            + Geometry.create(centralColombia).key()
    //            + " >>>> "
    //            + centralColombiaGeometry.key());

    var adapter = new WCSAdapter();
    var service =
        getService("https://integratedmodelling.org/geoserver/ows", Version.create("2.0.1"));
    var layer = service.getLayer("im-data-global-geography__elevation-global-90m");

    if (layer != null) {
      var coverage =
          adapter.getCoverage(
              layer,
              service,
              Observable.number("elevation"),
              Parameters.create(),
              GeometryRepository.INSTANCE.get(centralColombia, Geometry.class));

      System.out.println(coverage);
    }
  }
}
