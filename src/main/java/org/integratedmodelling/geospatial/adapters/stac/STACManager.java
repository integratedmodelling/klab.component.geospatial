package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONObject;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.hortonmachine.gears.io.stac.HMStacCollection;
import org.hortonmachine.gears.io.stac.HMStacItem;
import org.hortonmachine.gears.io.stac.HMStacManager;
import org.hortonmachine.gears.libs.modules.HMRaster;
import org.hortonmachine.gears.libs.monitor.LogProgressMonitor;
import org.hortonmachine.gears.utils.RegionMap;
import org.hortonmachine.gears.utils.geometry.GeometryUtilities;
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
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.runtime.scale.space.EnvelopeImpl;
import org.integratedmodelling.klab.runtime.scale.space.ProjectionImpl;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class STACManager {
    public static JSONObject requestMetadata(String collectionUrl, String type) {
        HttpResponse<JsonNode> response = Unirest.get(collectionUrl).asJson();
        if (!response.isSuccess() || response.getBody() == null) {
            throw new KlabResourceAccessException("Cannot access the " + type + " at " + collectionUrl);
        }
        JSONObject data = response.getBody().getObject();
        if (!data.getString("type").equalsIgnoreCase(type)) {
            throw new KlabResourceAccessException("Data at " + collectionUrl + " is not a valid STAC " + type);
        }
        return response.getBody().getObject();
    }

    // https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#common-media-types-in-stac
    final private static Set<String> SUPPORTED_MEDIA_TYPE = Set.of("image/tiff;application=geotiff", "image/vnd.stac.geotiff",
            "image/tiff;application=geotiff;profile=cloud-optimized", "image/vnd.stac.geotiff;profile=cloud-optimized",
            "image/vnd.stac.geotiff;cloud-optimized=true", "application/geo+json");

    final private static Set<String> SUPPORTED_MEDIA_EXTENSION = Set.of(".tif", ".tiff");

    /**
     * Check if the MIME value is supported.
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
            boolean isRasterExtension = RasterAdapter.fileExtensions.stream().anyMatch(ex -> href.toLowerCase().endsWith(ex));
            if (isRasterExtension) {
                return Artifact.Type.NUMBER;
            }
        }
        return Artifact.Type.VOID;
    }

    public static JSONObject getItems(JSONObject collection) throws Throwable {
        var links = collection.getJSONArray("links");
        JSONObject itemsObject = (JSONObject) links.toList().stream().filter(link -> ((JSONObject)link).getString("rel").equalsIgnoreCase("items"))
                .findFirst().orElseThrow(() -> new KlabResourceAccessException("STAC collection does not contain a link to the items object."));
        String itemsHref = itemsObject.getString("href");

        return requestMetadata(itemsHref, "FeatureCollection");
    }

    public static Set<String> readAssetNames(JSONObject assets) {
        return Set.of(JSONObject.getNames(assets));
    }

    public static JSONObject getAsset(JSONObject assetMap, String assetId) {
        return assetMap.getJSONObject(assetId);
    }

    public static GridCoverage2D getGridCoverage2D(Resource resource, Data.Builder builder, Geometry geometry) throws Exception {
        String collectionUrl = resource.getParameters().get("collection", String.class);
        JSONObject collectionData = STACManager.requestMetadata(collectionUrl, "collection");
        String collectionId = collectionData.getString("id");
        String catalogUrl = STACUtils.getCatalogUrl(collectionUrl, collectionId, collectionData);
        JSONObject catalogData = STACManager.requestMetadata(catalogUrl, "catalog");
        String assetId = resource.getParameters().get("asset", String.class);

        boolean hasSearchOption = STACUtils.containsLinkTo(catalogData, "search");
        if (!hasSearchOption) {
            throw new KlabUnimplementedException("Static catalogs are not implemented yet");
        }

        var space = (Space) geometry.getDimensions().stream().filter(d -> d instanceof Space).findFirst().orElseThrow();
        var envelope = space.getEnvelope();
        List<Double> bbox =  List.of(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
        var time = (Time) geometry.getDimensions().stream().filter(d -> d instanceof Time).findFirst().orElseThrow();
        var resourceTime = (Time) Scale.create(resource.getGeometry()).getTime();

        LogProgressMonitor lpm = new LogProgressMonitor();
        HMStacManager manager = new HMStacManager(catalogUrl, lpm);
        HMStacCollection collection = null;
        try {
            manager.open();
            collection = manager.getCollectionById(resource.getParameters().get("collectionId", String.class));
        } catch (Exception e) {
            throw new KlabResourceAccessException("Cannot access to STAC collection " + collectionUrl);
        }

        if (collection == null) {
            throw new KlabResourceAccessException("Collection " + resource.getParameters().get("collection", String.class) + " cannot be found.");
        }

        // TODO for now, we do not manage the semantics for the MergeMode
        HMRaster.MergeMode mergeMode = HMRaster.MergeMode.SUM;
        /*
        IObservable targetSemantics = scope.getTargetArtifact() instanceof Observation
                ? ((Observation) scope.getTargetArtifact()).getObservable()
                : null;
        HMRaster.MergeMode mergeMode = chooseMergeMode(targetSemantics, scope.getMonitor());
         */

        var env = EnvelopeImpl.create(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY(), space.getProjection());
        var poly = GeometryUtilities.createPolygonFromEnvelope(env.getJTSEnvelope());
        collection.setGeometryFilter(poly);

        // TODO check how to validate the time coverage
        var start = time.getStart();
        var end = time.getEnd();
        collection.setTimestampFilter(new Date(start.getMilliseconds()), new Date(end.getMilliseconds()));

        GridCoverage2D coverage = null;
        try {
            coverage = buildStacCoverage(builder, collection, mergeMode, space, envelope, assetId, lpm);
        } catch (Exception e) {
            throw new KlabResourceAccessException("Cannot build STAC raster output. Reason " + e.getMessage());
        } finally {
            manager.close();
        }
        return coverage;
    }

    private static GridCoverage2D buildStacCoverage(Data.Builder builder, HMStacCollection collection, HMRaster.MergeMode mergeMode, Space space, Envelope envelope, String assetId, LogProgressMonitor lpm) throws Exception {
        List<HMStacItem> items = collection.searchItems();

        if (items.isEmpty()) {
            throw new KlabIllegalStateException("No STAC items found for this context.");
        }
        builder.notification(Notification.debug("Found " + items.size() + " STAC items."));

        if (mergeMode == HMRaster.MergeMode.SUBSTITUTE) {
            sortByDate(items, builder);
        }

        // TODO check the usage of space.getStandardizedHeight() and space.getStandardizedHeight()
        RegionMap region = RegionMap.fromBoundsAndGrid(space.getEnvelope().getMinX(), space.getEnvelope().getMaxX(),
                envelope.getMinY(), envelope.getMaxY(), (int) space.getStandardizedWidth(),
                (int) space.getStandardizedHeight());

        ReferencedEnvelope regionEnvelope = new ReferencedEnvelope(region.toEnvelope(), ((ProjectionImpl) space.getProjection()).getCRS());
        RegionMap regionTransformed = RegionMap.fromEnvelopeAndGrid(regionEnvelope, (int) space.getStandardizedWidth(),
                (int) space.getStandardizedHeight());
        Set<Integer> EPSGsAtItems = items.stream().map(HMStacItem::getEpsg).collect(Collectors.toUnmodifiableSet());
        if (EPSGsAtItems.size() > 1) {
            builder.notification(Notification.warning("Multiple EPSGs found on the items " + EPSGsAtItems.toString() + ". The transformation process could affect the data."));
        }

        // Forget about AWS for now

        // Allow transform ensures the process to finish, but I would not bet on the resulting data.
        final boolean allowTransform = true;
        HMRaster outRaster = HMStacCollection.readRasterBandOnRegion(regionTransformed, assetId, items, allowTransform, mergeMode, lpm);
        return outRaster.buildCoverage();
    }

    private static void sortByDate(List<HMStacItem> items, Data.Builder builder) {
        if (items.stream().anyMatch(i -> i.getTimestamp() == null)) {
            throw new KlabIllegalStateException("STAC items are lacking a timestamp and could not be sorted by date.");
        }
        items.sort(Comparator.comparing(HMStacItem::getTimestamp));
        builder.notification(Notification.debug(
                "Ordered STAC items. First: [" + items.get(0).getTimestamp() + "]; Last [" + items.get(items.size() - 1).getTimestamp() + "]"));
    }

}
