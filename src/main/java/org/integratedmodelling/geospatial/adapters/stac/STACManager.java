package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.json.JSONObject;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.hortonmachine.gears.io.stac.HMStacCollection;
import org.hortonmachine.gears.io.stac.HMStacItem;
import org.hortonmachine.gears.io.stac.HMStacManager;
import org.hortonmachine.gears.libs.modules.HMRaster;
import org.hortonmachine.gears.libs.monitor.LogProgressMonitor;
import org.hortonmachine.gears.utils.CrsUtilities;
import org.hortonmachine.gears.utils.RegionMap;
import org.hortonmachine.gears.utils.geometry.GeometryUtilities;
import org.integratedmodelling.common.authentication.Authentication;
import org.integratedmodelling.geospatial.adapters.RasterAdapter;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.exceptions.KlabIllegalStateException;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;
import org.integratedmodelling.klab.api.exceptions.KlabUnimplementedException;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.knowledge.Artifact;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.observation.scale.Scale;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Envelope;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Space;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.Time;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.runtime.scale.space.EnvelopeImpl;
import org.integratedmodelling.klab.runtime.scale.space.ProjectionImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.*;
import java.util.stream.Collectors;

public class STACManager {

  // https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#common-media-types-in-stac
  private static final Set<String> SUPPORTED_MEDIA_TYPE =
      Set.of(
          "image/tiff;application=geotiff",
          "image/vnd.stac.geotiff",
          "image/tiff;application=geotiff;profile=cloud-optimized",
          "image/vnd.stac.geotiff;profile=cloud-optimized",
          "image/vnd.stac.geotiff;cloud-optimized=true",
          "application/geo+json");

  private static final Set<String> SUPPORTED_MEDIA_EXTENSION = Set.of(".tif", ".tiff");

  /**
   * Check if the MIME value is supported.
   *
   * @param asset as JSON
   * @return true if the media type is supported.
   */
  public static boolean isSupportedMediaType(JSONObject asset) {
    if (!asset.has("type")) {
      String href = asset.getString("href");

      return SUPPORTED_MEDIA_EXTENSION.stream().anyMatch(ex -> href.toLowerCase().endsWith(ex));
    }
    return SUPPORTED_MEDIA_TYPE.contains(asset.getString("type").replace(" ", "").toLowerCase());
  }

  public static Artifact.Type getArtifactType(JSONObject asset) {
    if (!asset.has("type")) {
      String href = asset.getString("href");
      boolean isRasterExtension =
          RasterAdapter.fileExtensions.stream().anyMatch(ex -> href.toLowerCase().endsWith(ex));
      if (isRasterExtension) {
        return Artifact.Type.NUMBER;
      }
    }
    return Artifact.Type.VOID;
  }

  public static GridCoverage2D getGridCoverage2D(
      Resource resource, Data.Builder builder, Geometry geometry, Scope scope) throws Exception {
    String collectionUrl = resource.getParameters().get("collection", String.class);
    JSONObject collectionData = STACParser.requestMetadata(collectionUrl, "collection");
    String collectionId = collectionData.getString("id");
    String catalogUrl = STACParser.getCatalogUrl(collectionUrl, collectionId, collectionData);
    JSONObject catalogData = STACParser.requestMetadata(catalogUrl, "catalog");
    String assetId = resource.getParameters().get("asset", String.class);

    var space =
        (Space)
            geometry.getDimensions().stream()
                .filter(d -> d instanceof Space)
                .findFirst()
                .orElseThrow(); // (Space) geometry.dimension(Geometry.Dimension.Type.SPACE)
    var envelope = space.getEnvelope();
    List<Double> bbox =
        List.of(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
    var time =
        (Time)
            geometry.getDimensions().stream()
                .filter(d -> d instanceof Time)
                .findFirst()
                .orElseThrow();
    var resourceTime = (Time) Scale.create(resource.getGeometry()).getTime();

    boolean hasSearchOption = STACParser.containsLinkTo(catalogData, "search");
    if (!hasSearchOption) {
      try {
        var features = STACParser.getFeaturesFromStaticCollection(collectionData);

        // Filter by time
        features = features.stream().filter(f -> isFeatureInTimeRange(time, f)).toList();

        // Filter by space
        features =
            features.stream()
                .filter(
                    f -> {
                      return true; // TODO
                    })
                .toList();

        CoordinateReferenceSystem crs =
            features.get(0).getFeatureType().getCoordinateReferenceSystem();
        if (crs == null) {
          crs = CrsUtilities.getCrsFromSrid(4326); // We go to the standard
        }

        // To HM items
        List<HMStacItem> items =
            features.stream()
                .map(
                    f -> {
                      try {
                        return HMStacItem.fromSimpleFeature(f);
                      } catch (Exception e) {
                        builder.notification(
                            Notification.warning(
                                "Cannot parse feature " + f.getID() + ". Ignored."));
                        return null;
                      }
                    })
                .filter(Objects::nonNull)
                .toList();

        // RegionMap regionTransformed = RegionMap.fromEnvelopeAndGrid(space.getEnvelope(),
        // space.getStandardizedWidth(), space.getStandardizedHeight());
        // HMRaster outRaster = HMStacCollection.readRasterBandOnRegion(regionTransformed, assetId,
        // items, true, HMRaster.MergeMode.SUBSTITUTE, new LogProgressMonitor());
        // TODO keep working on it
        throw new KlabUnimplementedException("Static collections cannot be imported.");
      } catch (Throwable e) {
        // TODO
        throw new RuntimeException(e);
      }
    }

    LogProgressMonitor lpm = new LogProgressMonitor();
    HMStacManager manager = new HMStacManager(catalogUrl, lpm);
    HMStacCollection collection = null;
    try {
      manager.open();
      collection =
          manager.getCollectionById(resource.getParameters().get("collectionId", String.class));
    } catch (Exception e) {
      throw new KlabResourceAccessException("Cannot access to STAC collection " + collectionUrl);
    }

    if (collection == null) {
      throw new KlabResourceAccessException(
          "Collection "
              + resource.getParameters().get("collection", String.class)
              + " cannot be found.");
    }

    // TODO for now, we do not manage the semantics for the MergeMode
    HMRaster.MergeMode mergeMode = HMRaster.MergeMode.SUM;
    /*
    IObservable targetSemantics = scope.getTargetArtifact() instanceof Observation
            ? ((Observation) scope.getTargetArtifact()).getObservable()
            : null;
    HMRaster.MergeMode mergeMode = chooseMergeMode(targetSemantics, scope.getMonitor());
     */

    var env =
        EnvelopeImpl.create(
            envelope.getMinX(),
            envelope.getMaxX(),
            envelope.getMinY(),
            envelope.getMaxY(),
            space.getProjection());
    var poly = GeometryUtilities.createPolygonFromEnvelope(env.getJTSEnvelope());
    collection.setGeometryFilter(poly);

    // TODO check how to validate the time coverage
    var start = time.getStart();
    var end = time.getEnd();
    collection.setTimestampFilter(
        new Date(start.getMilliseconds()), new Date(end.getMilliseconds()));

    GridCoverage2D coverage = null;
    try {
      // TODO working on S3 credentials
      var assets = STACParser.readAssetsFromCollection(collectionUrl, collectionData);
      Set<String> assetIds = STACParser.readAssetNames(assets);
      var asset = STACParser.getAsset(assets, assetId);
      String assetHref = asset.getString("href");
      if (assetHref.startsWith("s3://")) { // TODO manage S3 from the core projcet
        final String AWS_ENDPOINT =
            "https://s3.amazonaws.com"; // TODO generalize to any S3 endpoint
        var s3Credentials = Authentication.INSTANCE.getCredentials(AWS_ENDPOINT, scope);
        final boolean hasCredentials = s3Credentials.getCredentials().isEmpty();
        if (!hasCredentials) {
          throw new KlabResourceAccessException(
              "Cannot access " + AWS_ENDPOINT + ", lacking needed credentials.");
        }
      }
      coverage =
          buildStacCoverage(builder, collection, mergeMode, space, envelope, assetId, lpm, scope);
    } catch (Exception e) {
      throw new KlabResourceAccessException(
          "Cannot build STAC raster output. Reason " + e.getMessage());
    } finally {
      manager.close();
    }
    return coverage;
  }

  private static boolean isFeatureInTimeRange(Time time2, SimpleFeature f) {
    Date datetime = (Date) f.getAttribute("datetime");
    if (datetime != null) {
      if (isDateWithinRange(time2, datetime)) {
        return true;
      }
    }

    Date itemStart = (Date) f.getAttribute("start_datetime");
    if (itemStart == null) {
      return false;
    }
    Date itemEnd = (Date) f.getAttribute("end_datetime");
    if (itemEnd == null) {
      return itemStart.toInstant().getEpochSecond() <= time2.getStart().getMilliseconds();
    }
    if (isDateWithinRange(time2, itemStart) || isDateWithinRange(time2, itemEnd)) {
      return true;
    }
    return false;
  }

  private static boolean isDateWithinRange(Time rangeTime, Date date) {
    Date start = new Date(rangeTime.getStart().getMilliseconds());
    Date end = new Date(rangeTime.getEnd().getMilliseconds());
    return date.after(start) && date.before(end);
  }

  // TODO reduce number of args
  private static GridCoverage2D buildStacCoverage(
      Data.Builder builder,
      HMStacCollection collection,
      HMRaster.MergeMode mergeMode,
      Space space,
      Envelope envelope,
      String assetId,
      LogProgressMonitor lpm,
      Scope scope)
      throws Exception {
    List<HMStacItem> items = collection.searchItems();

    if (items.isEmpty()) {
      throw new KlabIllegalStateException("No STAC items found for this context.");
    }
    builder.notification(Notification.debug("Found " + items.size() + " STAC items."));

    if (mergeMode == HMRaster.MergeMode.SUBSTITUTE) {
      sortByDate(items, builder);
    }

    // TODO check the usage of space.getStandardizedHeight() and space.getStandardizedHeight()
    RegionMap region =
        RegionMap.fromBoundsAndGrid(
            space.getEnvelope().getMinX(),
            space.getEnvelope().getMaxX(),
            envelope.getMinY(),
            envelope.getMaxY(),
            (int) space.getStandardizedWidth(),
            (int) space.getStandardizedHeight());

    ReferencedEnvelope regionEnvelope =
        new ReferencedEnvelope(
            region.toEnvelope(), ((ProjectionImpl) space.getProjection()).getCRS());
    RegionMap regionTransformed =
        RegionMap.fromEnvelopeAndGrid(
            regionEnvelope,
            (int) space.getStandardizedWidth(),
            (int) space.getStandardizedHeight());
    Set<Integer> EPSGsAtItems =
        items.stream().map(HMStacItem::getEpsg).collect(Collectors.toUnmodifiableSet());
    if (EPSGsAtItems.size() > 1) {
      builder.notification(
          Notification.warning(
              "Multiple EPSGs found on the items "
                  + EPSGsAtItems.toString()
                  + ". The transformation process could affect the data."));
    }

    // Allow transform ensures the process to finish, but I would not bet on the resulting data.
    final boolean allowTransform = true;
    HMRaster outRaster =
        collection.readRasterBandOnRegion(
            regionTransformed, assetId, items, allowTransform, mergeMode, lpm);
    return outRaster.buildCoverage();
  }

  private static void sortByDate(List<HMStacItem> items, Data.Builder builder) {
    if (items.stream().anyMatch(i -> i.getTimestamp() == null)) {
      throw new KlabIllegalStateException(
          "STAC items are lacking a timestamp and could not be sorted by date.");
    }
    items.sort(Comparator.comparing(HMStacItem::getTimestamp));
    builder.notification(
        Notification.debug(
            "Ordered STAC items. First: ["
                + items.get(0).getTimestamp()
                + "]; Last ["
                + items.get(items.size() - 1).getTimestamp()
                + "]"));
  }
}
