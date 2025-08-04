package org.integratedmodelling.geospatial.adapters;

import org.integratedmodelling.geospatial.adapters.raster.RasterEncoder;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.knowledge.*;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.resources.adapters.Importer;
import org.integratedmodelling.klab.api.services.resources.adapters.Parameter;
import org.integratedmodelling.klab.api.services.resources.adapters.ResourceAdapter;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.api.utils.Utils;
import org.opengis.coverage.grid.GridCoverage;

import java.util.Set;

/**
 * File-based rasters, not embeddable. The implementation should enable promotion to STAC or WCS on
 * publication to shared services, so that it can become embeddable.
 */
@ResourceAdapter(
    name = "raster",
    version = Version.CURRENT,
    type = Artifact.Type.NUMBER,
    parameters = {
      // TODO
      @Parameter(
          name = RasterAdapter.NODATA_PARAM,
          type = Artifact.Type.NUMBER,
          description = "No data value")
    })
public class RasterAdapter {

  public static final String NODATA_PARAM = "noData";
  public static final String BAND_PARAM = "band";
  public static final String INTERPOLATION_PARAM = "interpolation";
  public static final String TRANSFORM_PARAM = "transform";
  public static final String BANDMIXER_PARAM = "bandmixer";

  /** All recognized primary file extensions. */
  public static Set<String> fileExtensions = Set.of("tif", "tiff");

  /** All recognized secondary file extensions */
  public static Set<String> secondaryFileExtensions =
      Set.of("tfw", "prj", "tif.ovr", "tif.aux.xml", "txt", "pdf");

  /** Interpolation type for metadata */
  public static final String INTERPOLATION_TYPE_FIELD = "interpolation";

  /** Safe interpolation types with JAI name equivalent */
  public enum Interpolation {
    BILINEAR("bilinear"),
    NEAREST_NEIGHBOR("nearest"),
    BICUBIC("bicubic"),
    BICUBIC2("bicubic2");

    public String field;

    Interpolation(String fieldName) {
      this.field = fieldName;
    }

    public static Interpolation getDefaultForType(Observable semantics) {
      return switch (semantics.getDescriptionType()) {
        case QUANTIFICATION -> Interpolation.BICUBIC;
        case CATEGORIZATION, VERIFICATION, DETECTION -> Interpolation.NEAREST_NEIGHBOR;
        case VOID,
            INSTANTIATION,
            SIMULATION,
            ACKNOWLEDGEMENT,
            CONNECTION,
            CLASSIFICATION,
            CHARACTERIZATION ->
            throw new IllegalArgumentException(
                "Cannot interpolate data for " + semantics.getDescriptionType() + " observations");
      };
    }

    public static Interpolation fromField(String field) {
      for (Interpolation it : Interpolation.values()) {
        if (it.field.equals(field)) {
          return it;
        }
      }
      throw new IllegalArgumentException("Unknown interpolation type field: " + field);
    }
  }

  @ResourceAdapter.Encoder
  public void encode(
      Resource resource,
      Urn urn,
      Data.Builder builder,
      Geometry geometry,
      Observable observable,
      Scope scope) {
    builder.notification(Notification.debug("Encoding a raster."));
    GridCoverage coverage = RasterEncoder.INSTANCE.getCoverage(resource, geometry);

    RasterEncoder.INSTANCE.encodeFromCoverage(
        resource,
        Utils.Resources.overrideParameters(resource, urn),
        coverage,
        geometry,
        builder,
        observable,
        scope);
  }

  @Importer(
      schema = "geotiff.import",
      knowledgeClass = KlabAsset.KnowledgeClass.RESOURCE,
      description = "Imports a raster resource",
      mediaType = "image/tiff;application=geotiff",
      fileExtensions = {"tif", "tiff"})
  public static String importGeotiff() {
    return null;
  }

  @ResourceAdapter.Validator(phase = ResourceAdapter.Validator.LifecyclePhase.LocalImport)
  public Notification validateImported(Resource resource) {
    // TODO
    return Notification.info("Raster imported.", Notification.Outcome.Success);
  }
}
