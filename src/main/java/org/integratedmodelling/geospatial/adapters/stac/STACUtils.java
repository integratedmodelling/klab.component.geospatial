package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONArray;
import kong.unirest.json.JSONObject;
import org.geotools.data.geojson.GeoJSONReader;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class STACUtils {

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

    public static SimpleFeature getItemAsFeature(String itemUrl) throws IOException {
        HttpResponse<JsonNode> response = Unirest.get(itemUrl).asJson();
        return GeoJSONReader.parseFeature(response.getBody().toString());
    }
}
