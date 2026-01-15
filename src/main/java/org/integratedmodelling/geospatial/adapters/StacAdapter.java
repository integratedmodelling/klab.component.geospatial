package org.integratedmodelling.geospatial.adapters;

import kong.unirest.json.JSONObject;
import org.geotools.coverage.grid.GridCoverage2D;
import org.hortonmachine.gears.io.stac.HMStacManager;
import org.integratedmodelling.geospatial.adapters.raster.RasterEncoder;
import org.integratedmodelling.geospatial.adapters.stac.StacResource;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Metadata;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.geometry.impl.GeometryBuilder;
import org.integratedmodelling.klab.api.knowledge.*;
import org.integratedmodelling.klab.api.knowledge.observation.scale.Scale;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Projection;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.resources.ResourceSet;
import org.integratedmodelling.klab.api.services.resources.adapters.Importer;
import org.integratedmodelling.klab.api.services.resources.adapters.Parameter;
import org.integratedmodelling.klab.api.services.resources.adapters.ResourceAdapter;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.api.services.runtime.extension.KlabFunction;

import java.time.Instant;
import java.util.Set;

/**
 * STAC is service-bound, so it can be embedded in a runtime.
 *
 * @author Ferd
 */
@ResourceAdapter(
    name = "stac",
    version = Version.CURRENT,
    embeddable = true,
    fillCurve = Data.FillCurve.D2_XY,
    minSizeForSplitting =
            1000000L, // TODO for now; could become configurable through runtime properties
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
      @Parameter(
          name = "band",
          type = Artifact.Type.NUMBER,
          optional = true,
          description =
            "For multiband rasters, this indicates the band to be retrieved."),
    })
public class StacAdapter {

  private static final Set<String> SUPPORTED_RASTER_MEDIA_TYPE =
      Set.of(
          "image/tiff;application=geotiff",
          "image/vnd.stac.geotiff",
          "image/tiff;application=geotiff;profile=cloud-optimized",
          "image/vnd.stac.geotiff;profile=cloud-optimized",
          "image/vnd.stac.geotiff;cloud-optimized=true");

  private static final Set<String> SUPPORTED_VECTOR_MEDIA_TYPE = Set.of("application/geo+json");

  public StacAdapter() {}

  @ResourceAdapter.Encoder
  public void encode(
      Resource resource,
      Urn urn,
      Data.Builder builder,
      Geometry geometry,
      Observable observable,
      Scope scope) {
    var collection = new StacResource.Collection(resource.getParameters().get("collection", String.class));
    var assetId = resource.getParameters().get("asset", String.class);
    var scale = Scale.create(geometry);
    var time = scale.getTime();
    var space = scale.getSpace();
    GridCoverage2D coverage = null;
    try {
      //coverage = collection.getCoverage(builder, space, time, assetId, scope);
      coverage = collection.getSTACCoverage(builder, space, time, assetId, scope);
    } catch (Exception e) {
      e.printStackTrace(); // get the stack trace
      builder.notification(
          Notification.error("Cannot encode STAC resource", Notification.Outcome.Failure));
      return;
    }
    RasterEncoder.INSTANCE.encodeFromCoverage(
        resource, Parameters.create(urn.getParameters()), coverage, geometry, builder);
  }

  @ResourceAdapter.Type
  public Artifact.Type getType(Resource resourceUrn) {
    // TODO get the data from the resource. For now, raster
    return Artifact.Type.NUMBER;
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
    return Artifact.Type.NUMBER;
  }


  @ResourceAdapter.Validator(phase = ResourceAdapter.Validator.LifecyclePhase.LocalImport)
  public boolean validateLocalImport(String collectionUrl) {
    var collection = new StacResource.Collection(collectionUrl);
    // For now we validate that it is a proper STAC collection
    return collection.isValid();
  }

  @Importer(
      schema = "stac.import",
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
        @KlabFunction.Argument(
            name = "band",
            type = Artifact.Type.NUMBER,
            optional = true,
            description = "Raster band."),
      })
  public static Resource importSTAC(Parameters<String> properties) {
    var collectionUrl = properties.get("collection", String.class);
    var assetId = properties.get("asset", String.class);
    var collection = new StacResource.Collection(collectionUrl);
    var urn = collectionUrl.substring(collection.getId().lastIndexOf("/") + 1) + "-" + assetId;

    var builder =
            Resource.builder(urn)
                    .withServiceId("geospatial")
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
    var bbox = extent.getJSONObject("spatial").getJSONArray("bbox").getJSONArray(0).toList();
    gBuilder.space().boundingBox(Double.parseDouble(bbox.get(0).toString()), Double.parseDouble(bbox.get(1).toString()),
            Double.parseDouble(bbox.get(2).toString()), Double.parseDouble(bbox.get(3).toString()));

    var interval = extent.getJSONObject("temporal").getJSONArray("interval").getJSONArray(0).toList();
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
