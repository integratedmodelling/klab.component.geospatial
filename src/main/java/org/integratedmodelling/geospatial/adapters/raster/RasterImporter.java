package org.integratedmodelling.geospatial.adapters.raster;

import java.awt.image.DataBuffer;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileWriter;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.styling.ColorMap;
import org.geotools.styling.ColorMapEntry;
import org.geotools.styling.RasterSymbolizer;
import org.integratedmodelling.kim.api.IParameters;
import org.integratedmodelling.klab.Logging;
import org.integratedmodelling.klab.Observations;
import org.integratedmodelling.klab.api.data.IGeometry.Dimension.Type;
import org.integratedmodelling.klab.api.data.ILocator;
import org.integratedmodelling.klab.api.data.IResource;
import org.integratedmodelling.klab.api.data.IResource.Builder;
import org.integratedmodelling.klab.api.data.adapters.IResourceImporter;
import org.integratedmodelling.klab.api.data.classification.IDataKey;
import org.integratedmodelling.klab.api.observations.IObservation;
import org.integratedmodelling.klab.api.observations.IState;
import org.integratedmodelling.klab.api.runtime.monitoring.IMonitor;
import org.integratedmodelling.klab.components.geospace.utils.GeotoolsUtils;
import org.integratedmodelling.klab.components.geospace.visualization.Renderer;
import org.integratedmodelling.klab.data.adapters.AbstractFilesetImporter;
import org.integratedmodelling.klab.ogc.RasterAdapter;
import org.integratedmodelling.klab.rest.Colormap;
import org.integratedmodelling.klab.rest.StateSummary;
import org.integratedmodelling.klab.utils.FileUtils;
import org.integratedmodelling.klab.utils.MiscUtilities;
import org.integratedmodelling.klab.utils.Pair;
import org.integratedmodelling.klab.utils.Parameters;
import org.integratedmodelling.klab.utils.Triple;
import org.integratedmodelling.klab.utils.ZipUtils;

import it.geosolutions.imageio.plugins.tiff.BaselineTIFFTagSet;

public class RasterImporter extends AbstractFilesetImporter {

    RasterValidator validator = new RasterValidator();
    IParameters<String> options = Parameters.create();

    public RasterImporter() {
        super(RasterAdapter.fileExtensions.toArray(new String[RasterAdapter.fileExtensions.size()]));
    }

    @Override
    public IResourceImporter withOption(String option, Object value) {

        /*
         * translate short options if any
         */
        if ("zip".equals(option)) {
            option = OPTION_DO_NOT_ZIP_MULTIPLE_FILES;
            value = (value instanceof Boolean) ? !((Boolean)value) : Boolean.FALSE;
        } else if ("remove".equals(option)) {
            option = OPTION_REMOVE_FILES_AFTER_ZIPPING;
        } else if ("folders".equals(option)) {
            value = (value instanceof Boolean) ? !((Boolean)value) : Boolean.FALSE;
            option = OPTION_DO_NOT_CREATE_INDIVIDUAL_FOLDERS;
            if (Boolean.TRUE.equals(value)) {
                this.options.put(OPTION_REMOVE_FILES_AFTER_ZIPPING, false);
                this.options.put(OPTION_DO_NOT_ZIP_MULTIPLE_FILES, true);
            }
        }

        this.options.put(option, value);
        return this;
    }

    @Override
    protected Builder importFile(String urn, File file, IParameters<String> userData, IMonitor monitor) {
        try {

            Builder builder = validator.validate(urn, file.toURI().toURL(), userData, monitor);

            if (builder != null) {
                String layerId = MiscUtilities.getFileBaseName(file).toLowerCase();
                builder.withLocalName(layerId).setResourceId(layerId);
                for (File f : validator.getAllFilesForResource(file)) {
                    builder.addImportedFile(f);
                }
            }

            return builder;

        } catch (MalformedURLException e) {
            Logging.INSTANCE.error(e);
            return null;
        }
    }

    @Override
    public List<Triple<String, String, String>> getExportCapabilities(IObservation observation) {
        List<Triple<String, String, String>> ret = new ArrayList<>();

        if (observation instanceof IState) {
            if (observation.getScale().getSpace() != null && observation.getScale().getSpace().isRegular()
                    && observation.getScale().isSpatiallyDistributed()) {
//                IState state = (IState) observation;
//                IDataKey dataKey = state.getDataKey();
//                if (dataKey != null) {
                    ret.add(new Triple<>("tiff", "GeoTIFF raster archive", "zip"));
//                } else {
//                    ret.add(new Triple<>("tiff", "GeoTIFF raster", "tiff"));
//                }
                ret.add(new Triple<>("png", "PNG image", "png"));
            }
        }

        return ret;
    }


    @Override
    public Map<String, String> getExportCapabilities(IResource resource) {
        Map<String, String> ret = new HashMap<>();
        ret.put("zip", "GeoTiff");
        return ret;
    }

//    @Override
//    public boolean exportResource(File file, IResource resource, String format) {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean importIntoResource(URL importLocation, IResource target, IMonitor monitor) {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean resourceCanHandle(IResource resource, String importLocation) {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean acceptsMultiple() {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//	@Override
//	public boolean write(Writer writer, IObservation observation, ILocator locator, IMonitor monitor) {
//		// TODO ASC format?
//		return false;
//	}

}
