package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONArray;
import kong.unirest.json.JSONObject;
import org.geotools.data.geojson.GeoJSONReader;
import org.hortonmachine.gears.io.stac.HMStacItem;
import org.integratedmodelling.klab.api.exceptions.KlabIOException;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;
import org.integratedmodelling.klab.api.exceptions.KlabValidationException;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class STACParser {

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

    public static List<HMStacItem> getHMItemsFromStaticCollection(JSONObject collectionData) {
        var collectionUrl = getLinkTo(collectionData, "self").get();
        var collectionId = collectionData.getString("id");
        var itemHrefs = getLinksTo(collectionData, "item");
        itemHrefs = itemHrefs.stream().map(href -> getUrlOfItem(collectionUrl, collectionId, href)).collect(Collectors.toUnmodifiableList());

        return itemHrefs.stream().map(i -> {
            try {
                return HMStacItem.fromSimpleFeature(STACParser.getItemAsFeature(i));
            } catch (Exception e) {
                throw new KlabValidationException("Item at " + i + " cannot be parsed.");
            }
        }).toList();
    }

    private static SimpleFeature getItemAsFeature(String itemUrl) throws IOException {
        HttpResponse<JsonNode> response = Unirest.get(itemUrl).asJson();
        return GeoJSONReader.parseFeature(response.getBody().toString());
    }

    public static String readDescription(JSONObject json) {
        return json.getString("description");
    }

    public static String readKeywords(JSONObject json) {
        if (!json.has("keywords")) {
            return null;
        }
        List<String> keywords = json.getJSONArray("keywords").toList();
        return keywords.isEmpty() ? null : String.join(",", keywords);
    }

    final private static Set<String> DOI_KEYS_IN_STAC_JSON = Set.of("sci:doi", "assets.sci:doi", "summaries.sci:doi", "properties.sci:doi", "item_assets.sci:doi");

    public static String readDOI(JSONObject json) {
        Optional<String> doi = DOI_KEYS_IN_STAC_JSON.stream().filter(key -> json.has(key)).map(key -> json.getString(key)).findFirst();
        return doi.orElse(null);
    }

    public static String readTitle(JSONObject json) {
        return json.getString("title");
    }

    // TODO check if the DOIreader should come here
    /*
    public static String readDOIAuthors(String doi) {
        Set<String> authorsSet = DOIReader.readAuthors(doi);
        StringBuilder authors = new StringBuilder();
        authorsSet.forEach(a -> authors.append(a).append("\n"));
        return authors.toString().trim();
    }
     */

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

    public static String readLicense(JSONObject collection) {
        if (!collection.has("links")) {
            return null;
        }
        JSONArray links = collection.getJSONArray("links");
        for (int i = 0; i < links.length(); i++) {
            JSONObject link = links.getJSONObject(i);
            if (!link.has("rel") || !link.getString("rel").equals("license")) {
                continue;
            }
            // A link to the license is preferred
            if (link.has("href")) {
                return link.getString("href");
            }
            if (link.has("title")) {
                link.getString("title");
            }
        }
        return null;
    }

    /*
    public static Type inferValueType(String key) {
        if (StringUtils.isNumeric(key)) {
            return Type.NUMBER;
        } else if ("true".equalsIgnoreCase(key) || "false".equalsIgnoreCase(key)) {
            return Type.BOOLEAN;
        }
        // As we are reading a JSON, text is our safest default option
        return Type.TEXT;
    }
     */

    /**
     * Reads the collection data and extracts the link pointing to the root element (the catalog).
     *
     * @param collectionUrl
     * @param collectionId
     * @param collectionData
     * @return url of the catalog
     */
    public static String getCatalogUrl(String collectionUrl, String collectionId, JSONObject collectionData) {
        // The URL of the catalog is the root
        if (!collectionData.has("links")) {
            throw new KlabResourceAccessException("STAC collection is missing links. It is not fully complaiant and cannot be accessed by the adapter.");
        }
        JSONArray links = collectionData.getJSONArray("links");
        Optional<JSONObject> rootLink = links.toList().stream().filter(link -> ((JSONObject) link).getString("rel").equalsIgnoreCase("root")).findFirst();
        if (rootLink.isEmpty()) {
            throw new KlabResourceAccessException("STAC collection is missing a relationship to the root catalog");
        }
        String href = rootLink.get().getString("href");
        return getUrlOfItem(collectionUrl, collectionId, href);
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
    public static JSONObject readAssetsFromCollection(String collectionUrl, JSONObject collection) throws KlabResourceAccessException {
        String collectionId = collection.getString("id");
        String catalogUrl = STACParser.getCatalogUrl(collectionUrl, collectionId, collection);
        JSONObject catalogData = STACParser.requestMetadata(catalogUrl, "catalog");

        Optional<String> searchEndpoint = STACParser.containsLinkTo(catalogData, "search")
                ? STACParser.getLinkTo(catalogData, "search")
                : STACParser.getLinkTo(collection, "search");

        // Static catalogs should have their assets on the Collection
        if (searchEndpoint.isEmpty()) {
            // Check the assets
            if (collection.has("assets")) {
                return collection.getJSONObject("assets");
            }
            // Try to get the assets from a link that has type `item`
            Optional<String> itemHref = STACParser.getLinkTo(collection, "item");
            if (itemHref.isEmpty()) {
                throw new KlabIOException("Cannot find items at STAC collection \"" + collectionUrl + "\"");
            }
            String itemUrl = itemHref.get().startsWith(".")
                    ? collectionUrl.replace("collection.json", "") + itemHref.get().replace("./", "")
                    : itemHref.get();
            // TODO get assets from the item
            JSONObject itemData = STACParser.requestMetadata(itemUrl, "feature");
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
        if (searchResponse.getJSONArray("features").length() == 0) {
            throw new KlabResourceAccessException(); // TODO set message there is no feature
        }

        return searchResponse.getJSONArray("features").getJSONObject(0).getJSONObject("assets");
    }
}
