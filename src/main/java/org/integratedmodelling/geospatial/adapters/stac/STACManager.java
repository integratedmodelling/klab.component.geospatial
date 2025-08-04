package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONObject;
import org.integratedmodelling.geospatial.adapters.RasterAdapter;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;
import org.integratedmodelling.klab.api.knowledge.Artifact;

import java.util.Set;

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

}
