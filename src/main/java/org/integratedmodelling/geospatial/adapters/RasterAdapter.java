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

    /**
     * All recognized primary file extensions.
     */
    public static Set<String> fileExtensions = Set.of("tif", "tiff");

    /**
     * All recognized secondary file extensions
     */
    public static Set<String> secondaryFileExtensions =
            Set.of("tfw", "prj", "tif.ovr", "tif.aux.xml", "txt", "pdf");

    /**
     * All the permitted band mixing operations.
     */
    public static Set<String> bandMixingOperations =
            Set.of("max_value", "min_value", "avg_value", "max_band", "min_band");

    /**
     * Interpolation type for metadata
     */
    public static final String INTERPOLATION_TYPE_FIELD = "interpolation";

    /**
     * Possible values of interpolation type (JAI classes)
     */
    public static final String[] INTERPOLATION_TYPE_VALUES = {
            "bilinear", "nearest", "bicubic", "bicubic2"
    };

    @ResourceAdapter.Encoder
    public void encode(
            Resource resource, Urn urn, Data.Builder builder, Geometry geometry, Observable observable, Scope scope) {
        builder.notification(Notification.debug("Encoding a raster."));
        GridCoverage coverage = new RasterEncoder().getCoverage(resource, geometry);

        new RasterEncoder().encodeFromCoverage(resource, urn.getParameters(), coverage, geometry, builder, scope);
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
