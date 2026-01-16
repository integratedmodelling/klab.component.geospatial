package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONArray;
import kong.unirest.json.JSONObject;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.geojson.GeoJSONReader;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.hortonmachine.gears.io.rasterreader.OmsRasterReader;
import org.hortonmachine.gears.io.stac.HMStacItem;
import org.hortonmachine.gears.io.stac.HMStacManager;
import org.hortonmachine.gears.libs.modules.HMRaster;
import org.hortonmachine.gears.libs.monitor.LogProgressMonitor;
import org.hortonmachine.gears.utils.RegionMap;
import org.hortonmachine.gears.utils.geometry.GeometryUtilities;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.exceptions.KlabIllegalStateException;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Projection;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Space;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.Time;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.runtime.scale.space.EnvelopeImpl;
import org.integratedmodelling.klab.runtime.scale.space.ProjectionImpl;
import org.locationtech.jts.geom.Geometry;

import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

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
        private final Optional<String> searchEndpoint;

        public String getUrl() {
            return url;
        }

        public String getId() {
            return id;
        }

        public JSONObject getData() {
            return data;
        }

        public Optional<String> getSearchEndpoint() {
            return searchEndpoint;
        }

        public boolean hasSearchEndpoint() {
            return searchEndpoint.isPresent();
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
            this.searchEndpoint = getLinkTo("search");
        }

        private Optional<String> getLinkTo(String rel) {
            return data.getJSONArray("links").toList().stream()
                    .filter(link -> ((JSONObject) link).getString("rel").equalsIgnoreCase(rel)).map(link -> ((JSONObject) link).getString("href"))
                    .findFirst();
        }
    }

    public static class Collection {
        private final String url;
        private String id;
        private final JSONObject data;
        private Catalog catalog = null;
        private List<Item> assetNames;

        private Optional<String> keywords;
        private Optional<String> doi;
        private Optional<String> description;
        private Optional<String> license;
        private Optional<String> title;

        final private static Set<String> DOI_KEYS = Set.of("sci:doi", "assets.sci:doi", "summaries.sci:doi", "properties.sci:doi", "item_assets.sci:doi");

        // https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#common-media-types-in-stac
        final private static Set<String> SUPPORTED_RASTER_MEDIA_TYPE =
                Set.of(
                        "image/tiff;application=geotiff",
                        "image/vnd.stac.geotiff",
                        "image/tiff;application=geotiff;profile=cloud-optimized",
                        "image/vnd.stac.geotiff;profile=cloud-optimized",
                        "image/vnd.stac.geotiff;cloud-optimized=true");
        final private static Set<String> SUPPORTED_VECTOR_MEDIA_TYPE = Set.of("application/geo+json", "application/vnd.shp", "application/gml+xml");

        // Keep in mind that .json and .xml might not be vector files
        private static final Set<String> SUPPORTED_MEDIA_EXTENSION = Set.of(".tif", ".tiff",
                ".shp", ".shx", ".dbf", ".json", ".geojson", "gml", "xml");


        /**
         * Check if the MIME value is supported.
         *
         * @param asset as JSON
         * @return true if the media type is supported.
         */
        private static boolean isSupportedMediaType(JSONObject asset) {
            if (!asset.has("type")) {
                String href = asset.getString("href");

                return SUPPORTED_MEDIA_EXTENSION.stream().anyMatch(ex -> href.toLowerCase().endsWith(ex));
            }

            return SUPPORTED_RASTER_MEDIA_TYPE.contains(asset.getString("type").replace(" ", "").toLowerCase())
                    || SUPPORTED_VECTOR_MEDIA_TYPE.contains(asset.getString("type").replace(" ", "").toLowerCase());
        }


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
            var keywordList = data.getJSONArray("keywords").toList();
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
                    .findFirst().orElseThrow(() -> new KlabResourceAccessException("Cannot find the Url of Catalog " + id));
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

        static final Set<String> requiredFieldsOfCollection =
                Set.of("type", "stac_version", "id", "description", "license", "extent", "links");

        /**
         * Validates if the STAC collection has the required fields and the correct JSON type.
         * @return true if no issue is found.
         */
        public boolean isValid() {
            boolean hasRequiredFields = requiredFieldsOfCollection.stream().anyMatch(data::has);
            if (!hasRequiredFields) {
                nonStandardWarnings.add("The STAC collection " + this.id + " is missing required fields.");
                return false;
            }
            if (!data.has("type") || !data.getString("type").equalsIgnoreCase("collection")) {
                nonStandardWarnings.add("The STAC collection " + this.id + " is does not have type=collection.");
                return false;
            }
            return true;
        }

        public GridCoverage2D getSTACCoverage(Data.Builder builder, Space space, Time time, String assetId, Scope scope) throws Exception {
            GridCoverage2D gridCoverage = null;
            try {
                var envelope = space.getEnvelope();
                double[] bbox = {envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()};
                var start = time.getStart();
                var end = time.getEnd();
                gridCoverage = getDataFromCOllection(
                        url, bbox, assetId, start.toString(), end.toString());

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            return gridCoverage;
        }

        private static GridCoverage2D getDataFromCOllection(String collectionURL, double[] bbox, String assetId, String startTime, String endTime) throws Exception {

            File coverageFile = File.createTempFile("geo", ".tiff");
            coverageFile.deleteOnExit();
            JSONObject collectionPostReq = new JSONObject();
            collectionPostReq.put("collection_url", collectionURL); // The url to the COG
            collectionPostReq.put("bbox", bbox);
            collectionPostReq.put("asset", assetId);
            collectionPostReq.put("start_time", startTime);
            collectionPostReq.put("end_time", endTime);

            kong.unirest.HttpResponse<File> stacQuerierResponse = Unirest
                    .post("https://stac-utils.integratedmodelling.org/stac_query")
                    .header("Content-Type", "application/json").body(collectionPostReq)
                    .connectTimeout(600000).socketTimeout(600000).asObject(r -> {
                        try (InputStream in = r.getContent(); OutputStream out = new FileOutputStream(coverageFile)) {
                            byte[] buffer = new byte[8192];
                            int bytesRead;
                            while ((bytesRead = in.read(buffer)) != -1) {
                                out.write(buffer, 0, bytesRead);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("Error writing response to file", e);
                        }
                        return coverageFile;
                    });

            return OmsRasterReader.readRaster(coverageFile.getAbsolutePath());
        }

        public GridCoverage2D getCoverage(Data.Builder builder, Space space, Time time, String assetId, Scope scope) throws Exception {
            LogProgressMonitor lpm = new LogProgressMonitor();
            var manager = new HMStacManager(catalog.getUrl(), lpm);
            manager.open();

            var collection = manager.getCollectionById(id);

            var str = space.toString();

            var envelope = space.getEnvelope();
            var env = EnvelopeImpl.create(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY(), space.getProjection());
            var poly = GeometryUtilities.createPolygonFromEnvelope(env.getJTSEnvelope()).convexHull();
            //GeometryRepository.INSTANCE.geometry(poly);
            //collection.setGeometryFilter(poly);
            double[] bbox = {envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()};
            collection.setBboxFilter(bbox);
            var start = time.getStart();
            var end = time.getEnd();
            collection.setTimestampFilter(
                    new Date(start.getMilliseconds()), new Date(end.getMilliseconds()));

            // TODO for now, we do not manage the semantics for the MergeMode
            HMRaster.MergeMode mergeMode = HMRaster.MergeMode.SUBSTITUTE;


            //return buildStacCoverage(builder, collection, mergeMode, space, envelope, assetId, lpm, scope);


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

            // Allow transform ensures the process to finish, but we shouldn't bet on the resulting data.
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

    public class Item {
        private final String url;
        private final String id;
        private final JSONObject data;
        // Geometry or bbox
        private final Geometry geometry;
        private final Instant start;
        private final Instant end;

        private List<Asset> assets;

        public String getId() {
            return id;
        }

        public List<Asset> getAssets() {
            return assets;
        }

        public Item(String url) {
            this.url = url;
            HttpResponse<JsonNode> response = Unirest.get(url).asJson();
            if (!response.isSuccess() || response.getBody() == null) {
                throw new KlabResourceAccessException("Cannot access the collection at " + url);
            }
            this.data = response.getBody().getObject();
            if (data.has("type") || !data.getString("type").equalsIgnoreCase("Feature")) {
                nonStandardWarnings.add("The STAC collection " + this.url + " is does not have type=item.");
                throw new KlabResourceAccessException("Data at " + url + " is not a valid STAC item");
            }

            this.id = data.getString("id");

            if (data.has("geometry") && data.get("geometry") != null) {
                geometry = GeoJSONReader.parseGeometry(data.getString("geometry"));
            } else if (data.has("bbox") && data.get("bbox") != null) {
                List<Double> bbox = data.getJSONArray("bbox").toList();
                var envelope = EnvelopeImpl.create(bbox.get(0), bbox.get(1), bbox.get(2), bbox.get(3), Projection.of(Projection.DEFAULT_PROJECTION_CODE));
                geometry = envelope.asJTSGeometry();
            } else {
                throw new KlabResourceAccessException("There is no geometry or bbox definition at the item.");
            }

            var start_date = data.getJSONObject("properties").getString("start_date");
            this.start = start_date == null ? null : Instant.parse(start_date);
            var end_date = data.getJSONObject("properties").getString("start_date");
            this.end = end_date == null ? null : Instant.parse(end_date);

            // TODO set assets
            this.assets = List.of();
        }

    }

    public class Asset {
        private String type;
        private String href;

        public Asset() {
            // TODO
        }

        public String getType() {
            return type;
        }

        public String getHref() {
            return href;
        }
    }

    /**
     * I am fed up with strange STAC implementations. I will try to support them, but I want to keep track of them somehow.
     * Then, we can tell it to the data providers.
     */
    private static List<String> nonStandardWarnings = new Vector<>();
}
