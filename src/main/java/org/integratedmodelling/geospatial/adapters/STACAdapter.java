package org.integratedmodelling.geospatial.adapters;

import kong.unirest.json.JSONObject;
import org.integratedmodelling.geospatial.adapters.stac.STACManager;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.exceptions.KlabUnimplementedException;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.knowledge.Artifact;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.Urn;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.resources.adapters.Parameter;
import org.integratedmodelling.klab.api.services.resources.adapters.ResourceAdapter;
import org.integratedmodelling.klab.api.services.resources.impl.ParameterImpl;

import java.util.Set;

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
    public void encode(Urn urn, Data.Builder builder, Geometry geometry, Observable observable, Scope scope) {
        // TODO
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

        if (SUPPORTED_RASTER_MEDIA_TYPE.contains(assetType.toLowerCase().replaceAll(" ", ""))) {
            return Artifact.Type.NUMBER;
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
