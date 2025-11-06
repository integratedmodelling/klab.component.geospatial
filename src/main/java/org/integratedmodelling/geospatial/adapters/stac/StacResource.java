package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONObject;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This class represents a k.LAB STAC resource. It heavily uses the HortonMachine implementation.
 * Most of our adaptations are related to the need of taking extra metadata from the JSON files that
 * describe the STAC resource.
 */
public class StacResource {
    private class Catalog {
        String url;
        String id;
        JSONObject data;

        public Catalog(String url) {
            this.url = url;
            HttpResponse<JsonNode> response = Unirest.get(url).asJson();
            if (!response.isSuccess() || response.getBody() == null) {
                throw new KlabResourceAccessException("Cannot access the catalog at " + url);
            }
            this.data = response.getBody().getObject();
            if (!data.getString("type").equalsIgnoreCase("catalog")) {
                throw new KlabResourceAccessException("Data at " + url + " is not a valid STAC catalog");
            }

            this.id = data.getString("id");
        }
    }

    private class Collection {
        private String url;
        private String id;
        private JSONObject data;
        private Catalog catalog;

        private Optional<String> keywords;
        private Optional<String> doi;
        private Optional<String> description;

        final private static Set<String> DOI_KEYS = Set.of("sci:doi", "assets.sci:doi", "summaries.sci:doi", "properties.sci:doi", "item_assets.sci:doi");

        public Collection(String url) {
            this.url = url;
            HttpResponse<JsonNode> response = Unirest.get(url).asJson();
            if (!response.isSuccess() || response.getBody() == null) {
                throw new KlabResourceAccessException("Cannot access the collection at " + url);
            }
            this.data = response.getBody().getObject();
            if (!data.getString("type").equalsIgnoreCase("collection")) {
                throw new KlabResourceAccessException("Data at " + url + " is not a valid STAC collection");
            }

            this.id = data.getString("id");

            // TODO is this the best wayto set catalog?
            String catalogUrl;
            try {
                catalogUrl = getCatalogUrl();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }

            this.catalog = new Catalog(catalogUrl);
        }

        public Optional<String> getKeywords() {
            if (keywords.isPresent()) {
                return keywords;
            }

            if (!data.has("keywords")) {
                return Optional.empty();
            }
            List keywordList = data.getJSONArray("keywords").toList();
            return keywords = keywordList.isEmpty() ? Optional.empty() : Optional.of(String.join(",", keywordList));
        }

        public Optional<String> getDoi() {
            if (doi.isPresent()) {
                return doi;
            }
            return doi = DOI_KEYS.stream().filter(data::has).map(data::getString).findFirst();
        }

        public Optional<String> getDescription() {
            if (description.isPresent()) {
                return description;
            }
            return description = data.has("description") ? Optional.of(data.getString("description")) : Optional.empty();
        }

        private String getCatalogUrl() throws Throwable {
            return (String) data.getJSONArray("links").toList()
                    .stream().filter(link -> ((JSONObject) link).getString("rel").equalsIgnoreCase("root")).map(link -> ((JSONObject) link).getString("href"))
                    .findFirst().orElseThrow(() -> new KlabResourceAccessException("Cannot find the Catalog of collection " + collection.getId()));
        }

    }

    private class Asset {
        String id;
        String type;
        String href;

        // TODO
    }

    private Catalog catalog;
    private Collection collection;
    private Asset asset;

    /**
     * I am fed up with strange STAC implementations. I will try to supportt them, but I want to keep track of them somehow.
     */
    private List<String> nonStandardWarnings;
}
