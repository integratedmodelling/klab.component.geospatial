package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONObject;
import org.geotools.data.geojson.GeoJSONReader;
import org.integratedmodelling.klab.api.exceptions.KlabIOException;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;
import org.integratedmodelling.klab.api.exceptions.KlabValidationException;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class StacParser {

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

    public static Set<String> readAssetNames(JSONObject assets) {
        return Set.of(JSONObject.getNames(assets));
    }

    public static JSONObject getAsset(JSONObject assetMap, String assetId) {
        return assetMap.getJSONObject(assetId);
    }

    public static List<SimpleFeature> getFeaturesFromStaticCollection(JSONObject collectionData) {
        var collectionUrl = getLinkTo(collectionData, "self").get();
        var collectionId = collectionData.getString("id");
        var itemHrefs = getLinksTo(collectionData, "item");
        itemHrefs = itemHrefs.stream().map(href -> getUrlOfItem(collectionUrl, collectionId, href)).toList();

        return itemHrefs.stream().map(i -> {
            try {
                return StacParser.getItemAsFeature(i);
            } catch (Exception e) {
                throw new KlabValidationException("Item at " + i + " cannot be parsed.");
            }
        }).toList();
    }

    private static SimpleFeature getItemAsFeature(String itemUrl) throws IOException {
        HttpResponse<JsonNode> response = Unirest.get(itemUrl).asJson();
        return GeoJSONReader.parseFeature(response.getBody().toString());
    }

    /**
     * Validates of the artifact contains a link to an element of type ref
     *
     * @param data of a collection, catalog or item
     * @param rel relation type
     * @return true if rel exists
     */
    public static boolean containsLinkTo(JSONObject data, String rel) {
        return data.getJSONArray("links").toList().stream().anyMatch(link -> ((JSONObject) link).getString("rel").equalsIgnoreCase(rel));
    }

    public static Optional<String> getLinkTo(JSONObject data, String rel) {
        return data.getJSONArray("links").toList().stream().filter(link -> ((JSONObject) link).getString("rel").equalsIgnoreCase(rel)).map(link -> ((JSONObject) link).getString("href")).findFirst();
    }

    public static List<String> getLinksTo(JSONObject data, String rel) {
        return data.getJSONArray("links").toList().stream().filter(link -> ((JSONObject) link).getString("rel").equalsIgnoreCase(rel)).map(link -> ((JSONObject) link).getString("href")).toList();
    }

    public static String getUrlOfItem(String collectionUrl, String collectionId, String href) {
        if (href.startsWith("..")) {
            return collectionUrl.replace("/collection.json", "").replace(collectionId, "") + href.replace("../", "");
        }
        if (href.startsWith(".")) {
            return collectionUrl.replace("collection.json", "") + href.replace("./", "");
        }
        return href;
    }

    /**
     * Reads the assets of a STAC collection and returns them as a JSON.
     * @param collection as a JSON
     * @return The asset list as a JSON
     * @throws KlabResourceAccessException
     */
    public static JSONObject readAssetsFromCollection(StacResource.Collection collection) throws KlabResourceAccessException {
        var catalog = collection.getCatalog();
        String collectionId = collection.getId();
        var collectionUrl = collection.getUrl();
        var collectionData = collection.getData();

        var searchEndpoint = catalog.getSearchEndpoint();
        // Static catalogs should have their assets on the Collection
        if (searchEndpoint.isEmpty()) {
            // Check the assets
            if (collectionData.has("assets")) {
                return collectionData.getJSONObject("assets");
            }
            // Try to get the assets from a link that has type `item`
            Optional<String> itemHref = StacParser.getLinkTo(collectionData, "item");
            if (itemHref.isEmpty()) {
                throw new KlabIOException("Cannot find items at STAC collection \"" + collectionUrl + "\"");
            }
            String itemUrl = itemHref.get().startsWith(".")
                    ? collectionUrl.replace("collection.json", "") + itemHref.get().replace("./", "")
                    : itemHref.get();
            // TODO get assets from the item
            JSONObject itemData = StacParser.requestMetadata(itemUrl, "feature");
            if (itemData.has("assets")) {
                return itemData.getJSONObject("assets");
            }
            throw new KlabIOException("Cannot find assets at STAC collection \"" + collectionUrl + "\"");
        }

        // TODO Move the query to another place.
        String parameters = "?collections=" + collectionId + "&limit=1";
        HttpResponse<JsonNode> response = Unirest.get(searchEndpoint.get() + parameters).asJson();

        if (!response.isSuccess()) {
            throw new KlabResourceAccessException(); //TODO set message
        }

        JSONObject searchResponse = response.getBody().getObject();
        if (searchResponse.getJSONArray("features").isEmpty()) {
            throw new KlabResourceAccessException(); // TODO set message there is no feature
        }

        return searchResponse.getJSONArray("features").getJSONObject(0).getJSONObject("assets");
    }
}
