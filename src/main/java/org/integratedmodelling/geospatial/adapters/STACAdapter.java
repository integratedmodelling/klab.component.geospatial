package org.integratedmodelling.geospatial.adapters;

import kong.unirest.json.JSONObject;
import org.geotools.coverage.grid.GridCoverage2D;
import org.hortonmachine.gears.io.stac.HMStacManager;
import org.integratedmodelling.geospatial.adapters.raster.RasterEncoder;
import org.integratedmodelling.geospatial.adapters.stac.STACManager;
import org.integratedmodelling.geospatial.adapters.stac.STACParser;
import org.integratedmodelling.geospatial.adapters.stac.StacResource;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Metadata;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.exceptions.KlabUnimplementedException;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.geometry.impl.GeometryBuilder;
import org.integratedmodelling.klab.api.knowledge.*;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Projection;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.resources.ResourceSet;
import org.integratedmodelling.klab.api.services.resources.adapters.Importer;
import org.integratedmodelling.klab.api.services.resources.adapters.Parameter;
import org.integratedmodelling.klab.api.services.resources.adapters.ResourceAdapter;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.api.services.runtime.extension.KlabFunction;

import java.time.Instant;
import java.util.List;
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
          description =
              "The URL pointing to the STAC collection file that contains the resource dataset."),
      @Parameter(
          name = "asset",
          type = Artifact.Type.TEXT,
          optional = true,
          description =
              "The asset that is going to be retrieved from the items. Left it blank when the information is stored in the feature."),
      // TODO manage bands
    })
public class STACAdapter {

  private static final Set<String> SUPPORTED_RASTER_MEDIA_TYPE =
      Set.of(
          "image/tiff;application=geotiff",
          "image/vnd.stac.geotiff",
          "image/tiff;application=geotiff;profile=cloud-optimized",
          "image/vnd.stac.geotiff;profile=cloud-optimized",
          "image/vnd.stac.geotiff;cloud-optimized=true");

  private static final Set<String> SUPPORTED_VECTOR_MEDIA_TYPE = Set.of("application/geo+json");

  public STACAdapter() {}

  @ResourceAdapter.Encoder
  public void encode(
      Resource resource,
      Urn urn,
      Data.Builder builder,
      Geometry geometry,
      Observable observable,
      Scope scope) {
    GridCoverage2D coverage = null;
    try {
      coverage = STACManager.getGridCoverage2D(resource, builder, geometry, scope);
    } catch (Exception e) {
      builder.notification(
          Notification.error("Cannot encode STAC resource", Notification.Outcome.Failure));
      return;
    }
    RasterEncoder.INSTANCE.encodeFromCoverage(
        resource, Parameters.create(urn.getParameters()), coverage, geometry, builder);
  }

  @ResourceAdapter.Type
  public Artifact.Type getType(Resource resourceUrn) {
    String collection = resourceUrn.getParameters().get("collection", String.class);
    var collectionData = STACParser.requestMetadata(collection, "collection");
    if (!resourceUrn.getParameters().contains("asset")
            || resourceUrn.getParameters().get("asset", String.class).isEmpty()) {
      // TODO get the assets from the links
      throw new KlabUnimplementedException("STAC adapter: can't handle static catalogs");
    }
    String assetId = resourceUrn.getParameters().get("asset", String.class);
    return getType(collection, assetId);
  }

    /**
     * STAC may provide all sorts of things, so the decision needs to look at the entire resource
     * parameterization.
     *
     * @param collection URL of the collection
     * @param assetId ID of the asset
     *
     * @return
     */
  @ResourceAdapter.Type
  public static Artifact.Type getType(String collection, String assetId) {
    JSONObject itemsData = null;
    try {
      // itemsData = StacParser.getSampleItem(collectionData); TODO
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

  static final Set<String> requiredFieldsOfCollection =
      Set.of("type", "stac_version", "id", "description", "license", "extent", "links");

  @ResourceAdapter.Validator(phase = ResourceAdapter.Validator.LifecyclePhase.LocalImport)
  public boolean validateLocalImport(String collection) {
    var collectionData = STACParser.requestMetadata(collection, "collection");
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

  @Importer(
      schema = "stac.import.v2",
      knowledgeClass = KlabAsset.KnowledgeClass.RESOURCE,
      description = "Imports a STAC resource",
      properties = {
        @KlabFunction.Argument(
            name = "collection",
            type = Artifact.Type.URL,
            description = "URL of the collection."),
        @KlabFunction.Argument(
            name = "asset",
            type = Artifact.Type.TEXT,
            optional = true,
            description = "Asset ID."),
        //
      })
  public static Resource importSTAC(Parameters<String> properties) {
    // TODO
    var collectionUrl = properties.get("collection", String.class);
    var assetId = properties.get("asset", String.class);

    StacResource.Collection collection = new StacResource.Collection(collectionUrl);

    var urn = collectionUrl.substring(collection.getId().lastIndexOf("/") + 1) + "-" + assetId;

    var builder =
            Resource.builder(urn)
                    //.withServiceId(service.serviceId())
                    .withParameters(properties)
                    .withAdapterType("stac")
                    .withType(getType(collectionUrl, assetId)); // We need to know if it is a raster or a vector or whatever

    // TODO add more metadata
    builder.withMetadata(Metadata.IM_KEYWORDS, collection.getKeywords())
            .withMetadata(Metadata.DC_NAME, collection.getTitle())
            .withMetadata("DOI", collection.getDoi())
            .withMetadata("license", collection.getLicense());

    var collectionId = collection.getId();

    var catalog = collection.getCatalog();
    HMStacManager manager = new HMStacManager(catalog.getUrl(), null);

    builder.withGeometry(readGeometry(collection.getData()));
    if (false) { // Manage the errors
      ResourceSet.empty(
              Notification.error("Cannot import the given STAC resource"));
    }
    return builder.build();
  }

  public static Geometry readGeometry(JSONObject collection) {
    GeometryBuilder gBuilder = Geometry.builder();

    JSONObject extent = collection.getJSONObject("extent");
    List bbox = extent.getJSONObject("spatial").getJSONArray("bbox").getJSONArray(0).toList();
    gBuilder.space().boundingBox(Double.valueOf(bbox.get(0).toString()), Double.valueOf(bbox.get(1).toString()),
            Double.valueOf(bbox.get(2).toString()), Double.valueOf(bbox.get(3).toString()));

    List interval = extent.getJSONObject("temporal").getJSONArray("interval").getJSONArray(0).toList();
    if (interval.get(0) != null) {
      gBuilder.time().start(Instant.parse(interval.get(0).toString()).toEpochMilli());
    }
    if (interval.size() > 1 && interval.get(1) != null) {
      gBuilder.time().end(Instant.parse(interval.get(1).toString()).toEpochMilli());
    }

    // TODO find non-ad-hoc cases
    if (collection.getString("id").equals("slovak_SK_v5_reference-points_EUNIS2012")) {
      return gBuilder.build().withProjection(Projection.DEFAULT_PROJECTION_CODE).withTimeType("logical");
    }
    return gBuilder.build().withProjection(Projection.DEFAULT_PROJECTION_CODE).withTimeType("grid");
  }

}
