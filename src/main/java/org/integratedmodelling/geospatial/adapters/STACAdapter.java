package org.integratedmodelling.geospatial.adapters;

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
import org.integratedmodelling.geospatial.adapters.raster.RasterEncoder;
import org.integratedmodelling.geospatial.adapters.stac.STACManager;
import org.integratedmodelling.geospatial.adapters.stac.STACUtils;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.exceptions.KlabIllegalStateException;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;
import org.integratedmodelling.klab.api.exceptions.KlabUnimplementedException;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.knowledge.Artifact;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.Urn;
import org.integratedmodelling.klab.api.knowledge.observation.scale.Scale;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Space;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.Time;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.resources.adapters.Parameter;
import org.integratedmodelling.klab.api.services.resources.adapters.ResourceAdapter;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.runtime.scale.space.EnvelopeImpl;
import org.integratedmodelling.klab.runtime.scale.space.ProjectionImpl;
import org.opengis.coverage.grid.GridCoverage;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * STAC is service-bound so it can be embedded in a runtime.
 *
 * @author Ferd
 */
@ResourceAdapter(
        name = "stac",
        version = Version.CURRENT,
        embeddable = true,
        parameters = {
            @Parameter(
                name = "collection",
                type = Artifact.Type.URL,
                description = "The URL pointing to the STAC collection file that contains the resource dataset."),
            @Parameter(
                name = "asset",
                type = Artifact.Type.TEXT,
                optional = true,
                description = "The asset that is going to be retrieved from the items. Left it blank when the information is stored in the feature."),
                // TODO manage bands
        })
public class STACAdapter {

    final private static Set<String> SUPPORTED_RASTER_MEDIA_TYPE = Set.of("image/tiff;application=geotiff", "image/vnd.stac.geotiff",
            "image/tiff;application=geotiff;profile=cloud-optimized", "image/vnd.stac.geotiff;profile=cloud-optimized",
            "image/vnd.stac.geotiff;cloud-optimized=true");

    final private static Set<String> SUPPORTED_VECTOR_MEDIA_TYPE = Set.of("application/geo+json");


    public STACAdapter() {
    }

    @ResourceAdapter.Encoder
    public void encode(Resource resource, Urn urn, Data.Builder builder, Geometry geometry, Observable observable, Scope scope) {
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

        var space = (Space) geometry.getDimensions().stream().filter(d -> d instanceof Space)
                .findFirst().orElseThrow();
        var envelope = space.getEnvelope();
        List<Double> bbox =  List.of(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
        var time = (Time) geometry.getDimensions().stream().filter(d -> d instanceof Time)
                .findFirst().orElseThrow();
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
            List<HMStacItem> items = collection.searchItems();

            if (items.isEmpty()) {
                manager.close();
                throw new KlabIllegalStateException("No STAC items found for this context.");
            }
            builder.notification(Notification.debug("Found " + items.size() + " STAC items."));

            if (mergeMode == HMRaster.MergeMode.SUBSTITUTE) {
                sortByDate(items, builder);
            }

            // TODO check the usage of space.getStandardizedHeight();
            RegionMap region = RegionMap.fromBoundsAndGrid(space.getEnvelope().getMinX(), space.getEnvelope().getMaxX(),
                    space.getEnvelope().getMinY(), space.getEnvelope().getMaxY(), (int) space.getStandardizedWidth(),
                    (int) space.getStandardizedHeight());

            ReferencedEnvelope regionEnvelope = new ReferencedEnvelope(region.toEnvelope(), ((ProjectionImpl)space.getProjection()).getCRS());
            RegionMap regionTransformed = RegionMap.fromEnvelopeAndGrid(regionEnvelope, (int) space.getStandardizedWidth(),
                    (int) space.getStandardizedHeight());
            Set<Integer> EPSGsAtItems = items.stream().map(HMStacItem::getEpsg).collect(Collectors.toUnmodifiableSet());
            if (EPSGsAtItems.size() > 1) {
                builder.notification(Notification.warning("Multiple EPSGs found on the items " + EPSGsAtItems.toString() + ". The transformation process could affect the data."));
            }

            // Forget about AWS for now

            // Allow transform ensures the process to finish, but I would not bet on the resulting
            // data.
            final boolean allowTransform = true;
            HMRaster outRaster = collection.readRasterBandOnRegion(regionTransformed, assetId, items, allowTransform, mergeMode, lpm);
            coverage = outRaster.buildCoverage();
            manager.close();
        } catch (Exception e) {
            throw new KlabResourceAccessException("Cannot build STAC raster output. Reason " + e.getMessage());
        }
        RasterEncoder.INSTANCE.encodeFromCoverage(resource, Parameters.create(urn.getParameters()), coverage, geometry, builder, observable, scope);
    }

    private void sortByDate(List<HMStacItem> items, Data.Builder builder) {
        if (items.stream().anyMatch(i -> i.getTimestamp() == null)) {
            throw new KlabIllegalStateException("STAC items are lacking a timestamp and could not be sorted by date.");
        }
        items.sort(Comparator.comparing(HMStacItem::getTimestamp));
        builder.notification(Notification.debug(
                "Ordered STAC items. First: [" + items.get(0).getTimestamp() + "]; Last [" + items.get(items.size() - 1).getTimestamp() + "]"));
    }

    /**
     * STAC may provide all sorts of things, so the decision needs to look at the entire resource
     * parameterization.
     *
     * @param resourceUrn
     * @return
     */
    @ResourceAdapter.Type
    public Artifact.Type getType(Resource resourceUrn) {
        String collection = resourceUrn.getParameters().get("collection", String.class);
        var collectionData = STACManager.requestMetadata(collection, "collection");
        if (!resourceUrn.getParameters().contains("asset") || resourceUrn.getParameters().get("asset", String.class).isEmpty()) {
            // TODO get the assets from the links
            throw new KlabUnimplementedException("STAC adapter: can't handle static catalogs");
        }
        String assetId = resourceUrn.getParameters().get("asset", String.class);
        JSONObject itemsData = null;
        try {
            itemsData = STACManager.getItems(collectionData);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        var assetsData = itemsData.getJSONArray("features").getJSONObject(0).getJSONObject("assets");
        if (!assetsData.has(assetId)) {
            throw new KlabUnimplementedException("STAC adapter: can't find " + assetId);
        }
        String assetType = assetsData.getJSONObject(assetId).getString("type");

        // TODO check if these are the correct types
        if (SUPPORTED_RASTER_MEDIA_TYPE.contains(assetType.toLowerCase().replaceAll(" ", ""))) {
            return Artifact.Type.NUMBER;
        }
        if (SUPPORTED_VECTOR_MEDIA_TYPE.contains(assetType.toLowerCase().replaceAll(" ", ""))) {
            return Artifact.Type.GEOMETRY;
        }
        // TODO other types
        throw new KlabUnimplementedException("STAC adapter: can't handle this type " + assetType);
    }

    final static Set<String> requiredFieldsOfCollection = Set.of("type", "stac_version", "id", "description", "license", "extent", "links");

    @ResourceAdapter.Validator(phase = ResourceAdapter.Validator.LifecyclePhase.LocalImport)
    public boolean validateLocalImport(String collection) {
        var collectionData = STACManager.requestMetadata(collection, "collection");
        // For now we validate that it is a proper STAC collection
        boolean hasRequiredFields = requiredFieldsOfCollection.stream().anyMatch(collectionData::has);
        if (!hasRequiredFields) {
            return false;
        }
        if (!collectionData.getString("type").equalsIgnoreCase("collection")) {
            return false;
        }
        return true;
    }

}
