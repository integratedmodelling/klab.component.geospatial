package org.integratedmodelling.geospatial.adapters;

import org.geotools.coverage.grid.GridCoverage2D;
import org.integratedmodelling.common.services.client.resources.ResourcesClient;
import org.integratedmodelling.geospatial.adapters.raster.RasterEncoder;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.knowledge.*;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.ResourcesService;
import org.integratedmodelling.klab.api.services.Service;
import org.integratedmodelling.klab.api.services.resources.adapters.Importer;
import org.integratedmodelling.klab.api.services.resources.adapters.Parameter;
import org.integratedmodelling.klab.api.services.resources.adapters.ResourceAdapter;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.configuration.ServiceConfiguration;
import org.opengis.coverage.grid.GridCoverage;

import java.util.Collection;
import java.util.List;
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

  /** All recognized primary file extensions. */
  public static Set<String> fileExtensions = Set.of("tif", "tiff");

  /** All recognized secondary file extensions */
  public static Set<String> secondaryFileExtensions =
      Set.of("tfw", "prj", "tif.ovr", "tif.aux.xml", "txt", "pdf");

  /** All the permitted band mixing operations. */
  public static Set<String> bandMixingOperations =
      Set.of("max_value", "min_value", "avg_value", "max_band", "min_band");

  /** Interpolation type for metadata */
  public static final String INTERPOLATION_TYPE_FIELD = "interpolation";

  /** Possible values of interpolation type (JAI classes) */
  public static final String[] INTERPOLATION_TYPE_VALUES = {
    "bilinear", "nearest", "bicubic", "bicubic2"
  };

  @ResourceAdapter.Encoder
  public void encode(
      Urn urn, Data.Builder builder, Geometry geometry, Observable observable, Scope scope) {
    //builder.notification(Notification.debug("Encoding a raster."));
    readRaster(urn, builder, geometry, observable, scope);
  }

  private void readRaster(
      Urn urn, Data.Builder builder, Geometry geometry, Observable observable, Scope scope) {
    //TODO move everything to a properly named file
    RasterEncoder encoder = new RasterEncoder();
    // TODO get the real Resource. For now, assume a single resource
    Collection<ResourcesService> resourceServices = scope.getServices(ResourcesService.class);

    Resource resource = null;
    for (var resourceService : resourceServices) {
      var resources = resourceService.resolveResource(List.of(urn.getUrn()), scope);
      resource = (Resource) resources.getResources().get(0);
    }
    GridCoverage coverage = encoder.getCoverage(resource, geometry);

    new RasterEncoder().encodeFromCoverage(resource, urn.getParameters(), coverage, geometry, builder, scope);
  }

  public static void main(String[] args) {
    System.out.println("Hi, raster!");

    // This sets up the k.LAB environment for the service side
    var ignored = ServiceConfiguration.INSTANCE.allowAnonymousUsage();

    String centralColombia =
        "τ0(1){ttype=LOGICAL,period=[1609459200000 1640995200000],tscope=1.0,"
            + "tunit=YEAR}S2(934,631){bbox=[-75.2281407807369 -72.67107290964314 3.5641500380320963 5"
            + ".302943221927137],"
            + "shape"
            + "=00000000030000000100000005C0522AF2DBCA0987400C8361185B1480C052CE99DBCA0987400C8361185B1480C052CE99DBCA098740153636BF7AE340C0522AF2DBCA098740153636BF7AE340C0522AF2DBCA0987400C8361185B1480,proj=EPSG:4326}";

    var observable = Observable.objects("porquerolles");
    var geometry = Geometry.create(centralColombia);
    var builder = Data.builder("colombia", observable, geometry);
    var adapter = new RasterAdapter();
    adapter.encode(Urn.of("klab:raster:test:colombia"), builder, geometry, observable, null);

    var built = builder.build();
    System.out.println(built);
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
}
