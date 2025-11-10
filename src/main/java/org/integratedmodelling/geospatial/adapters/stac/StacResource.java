package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONArray;
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
    public static class Catalog {
        private final String url;
        private final String id;
        private final JSONObject data;
        //private final String searchEndpoint;

        public String getUrl() {
            return url;
        }

        public String getId() {
            return id;
        }

        public JSONObject getData() {
            return data;
        }

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

            // TODO add search endpoint
        }
    }

    public static class Collection {
        private final String url;
        private final String id;
        private final JSONObject data;
        private final Catalog catalog;

        private Optional<String> keywords;
        private Optional<String> doi;
        private Optional<String> description;
        private Optional<String> license;
        private Optional<String> title;

        final private static Set<String> DOI_KEYS = Set.of("sci:doi", "assets.sci:doi", "summaries.sci:doi", "properties.sci:doi", "item_assets.sci:doi");

        final private static Set<String> SUPPORTED_RASTER_MEDIA_TYPE =
                Set.of(
                        "image/tiff;application=geotiff",
                        "image/vnd.stac.geotiff",
                        "image/tiff;application=geotiff;profile=cloud-optimized",
                        "image/vnd.stac.geotiff;profile=cloud-optimized",
                        "image/vnd.stac.geotiff;cloud-optimized=true");

        final private static Set<String> SUPPORTED_VECTOR_MEDIA_TYPE = Set.of("application/geo+json");

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

            // TODO is this the best way to set catalog?
            String catalogUrl;
            try {
                catalogUrl = getCatalogUrl();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }

            this.catalog = new Catalog(catalogUrl);
        }

        public String getUrl() {
            return url;
        }

        public String getId() {
            return id;
        }

        public JSONObject getData() {
            return data;
        }

        public Catalog getCatalog() {
            return catalog;
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

        public Optional<String> getTitle() {
            if (title.isPresent()) {
                return title;
            }
            return title = data.has("title") ? Optional.of(data.getString("title")) : Optional.empty();
        }

        private String getCatalogUrl() throws Throwable {
            return (String) data.getJSONArray("links").toList()
                    .stream().filter(link -> ((JSONObject) link).getString("rel").equalsIgnoreCase("root")).map(link -> ((JSONObject) link).getString("href"))
                    .findFirst().orElseThrow(() -> new KlabResourceAccessException("Cannot find the Catalog of collection " + collection.getId()));
        }

        public Optional<String> getLicense() {
            if (license.isPresent()) {
                return license;
            }

            if (!data.has("links")) {
                return Optional.empty();
            }
            JSONArray links = data.getJSONArray("links");
            for (int i = 0; i < links.length(); i++) {
                JSONObject link = links.getJSONObject(i);
                if (!link.has("rel") || !link.getString("rel").equals("license")) {
                    continue;
                }
                // A link to the license is preferred
                if (link.has("href")) {
                    return Optional.of(link.getString("href"));
                }
                if (link.has("title")) {
                    link.getString("title");
                }
            }
            return Optional.empty();
        }

    }

    public class Asset {
        String id;
        String type;
        String href;

        public Asset() {
            // TODO
        }

        public String getId() {
            return id;
        }

        public String getType() {
            return type;
        }

        public String getHref() {
            return href;
        }
    }

    private Catalog catalog;
    private static Collection collection;
    private Asset asset;

    /**
     * I am fed up with strange STAC implementations. I will try to supportt them, but I want to keep track of them somehow.
     */
    private List<String> nonStandardWarnings;
}
