/*
 * This file is part of k.LAB.
 *
 * k.LAB is free software: you can redistribute it and/or modify it under the terms of the Affero
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * A copy of the GNU Affero General Public License is distributed in the root directory of the k.LAB
 * distribution (LICENSE.txt). If this cannot be found see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2007-2018 integratedmodelling.org and any authors mentioned in author tags. All
 * rights reserved.
 */
package org.integratedmodelling.geospatial.adapters.raster;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.geotools.coverage.grid.GeneralGridEnvelope;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.coverage.processing.Operations;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.factory.Hints;
import org.integratedmodelling.common.utils.Utils;
import org.integratedmodelling.geospatial.adapters.RasterAdapter;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Metadata;
import org.integratedmodelling.klab.api.data.Storage;
import org.integratedmodelling.klab.api.exceptions.KlabIOException;
import org.integratedmodelling.klab.api.exceptions.KlabInternalErrorException;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.observation.scale.Scale;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.runtime.scale.space.EnvelopeImpl;
import org.integratedmodelling.klab.runtime.scale.space.ProjectionImpl;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import javax.media.jai.*;
import javax.media.jai.iterator.RandomIter;
import javax.media.jai.iterator.RandomIterFactory;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@code RasterEncoder} adapts a raster resource (file-based) to a passed geometry and builds
 * the correspondent Data buffer.
 */
public class RasterEncoder {

    /**
     * Take a Geotools coverage and do the rest. Separated so other adapters with raster result (WCS, STAC, OpenEO) can use it as is.
     *
     * @param resource
     * @param urnParameters
     * @param coverage
     * @param geometry
     * @param builder
     * @param scope
     */
    public void encodeFromCoverage(
            Resource resource,
            Map<String, String> urnParameters,
            GridCoverage coverage,
            Geometry geometry,
            Data.Builder builder,
            Scope scope) {

        /*
         * Set the data from the transformed coverage
         */
        var scale = Scale.create(geometry);
        RenderedImage image = coverage.getRenderedImage();
        RandomIter iterator = RandomIterFactory.create(image, null);
        var space = scale.getSpace();
        int band = 0;
        if (urnParameters.containsKey(RasterAdapter.BAND_PARAM)) {
            band = Integer.parseInt(urnParameters.get("band"));
        } else {
            resource.getParameters().get(RasterAdapter.BAND_PARAM, 0);
        }
        int nBands = coverage.getNumSampleDimensions();
        Set<Double> nodata = getNodata(resource, coverage, band);
        GroovyShell shell = null;
        Binding binding = null;
        Script transformation = null;

        if (resource.getParameters().get("transform") != null
                && !resource.getParameters().get("transform").toString().trim().isEmpty()) {
            binding = new Binding();
            shell = new GroovyShell(binding);
            transformation = shell.parse(resource.getParameters().get("transform").toString());
        }

        BandMixing.Operation bandMixer = null;
        if (resource.getParameters().contains("bandmixer")) {
            try {
                bandMixer = BandMixing.Operation.valueOf(resource.getParameters().get("bandmixer", String.class));
            } catch (IllegalArgumentException e) {
                builder.notification(Notification.error("Unsupported band mixing operation " + resource.getParameters().get("bandmixer", String.class)));
            }
        }

        var xy = scale.getSpace().getShape();
        var xx = xy.get(0);
        var yy = xy.get(1);
        long offset = 0;
        var filler = builder.buffer(Storage.DoubleBuffer.class, Data.SpaceFillingCurve.D2_XY);
        for (Geometry subscale : scale.without(Geometry.Dimension.Type.SPACE)) {
            var spaceScale = scale.at(subscale);
            for (int x = 0; x < xx; x++) {
                for (int y = 0; y < yy; y++) {

                    double value =
                            bandMixer == null
                                    ? getCellValue(iterator, x, y, band)
                                    : getCellMixerValue(iterator, x, y, bandMixer, nBands);

                    // this is cheeky but will catch most of the nodata and
                    // none of the good data
                    // FIXME see if this is really necessary
                    if (value < -1.0E35 || value > 1.0E35) {
                        value = Double.NaN;
                    }

                    for (double nd : nodata) {
                        if (Utils.Numbers.equal(value, nd)) {
                            value = Double.NaN;
                            break;
                        }
                    }

                    if (transformation != null && Utils.Data.isData(value)) {
                        binding.setVariable("self", value);
                        Object o = transformation.run();
                        if (o instanceof Number) {
                            value = ((Number) o).doubleValue();
                        } else {
                            value = Double.NaN;
                        }
                    }

                    filler.set(value, offset++);
                }
            }
        }
    }

    private double getCellValue(RandomIter iterator, long x, long y, int band) {
        return iterator.getSampleDouble((int) x, (int) y, band);
    }

    private double getCellMixerValue(
            RandomIter iterator, long x, long y, BandMixing.Operation operation, int nBands) {
        return switch (operation) {
            case MAX_VALUE -> getMaxCellValue(iterator, x, y, nBands);
            case MIN_VALUE -> getMinCellValue(iterator, x, y, nBands);
            case AVG_VALUE -> getAvgCellValue(iterator, x, y, nBands);
            case SUM_VALUE -> getSumCellValue(iterator, x, y, nBands);
            case BAND_MAX_VALUE -> getBandOfMaxValue(iterator, x, y, nBands);
            case BAND_MIN_VALUE -> getBandOfMinValue(iterator, x, y, nBands);
            default -> Double.NaN;
        };
    }

    private double getBandOfMaxValue(RandomIter iterator, long x, long y, int nBands) {
        double value = Double.NaN;
        double maxValue = Double.MIN_VALUE;
        for (int i = 0; i < nBands; i++) {
            double currentValue = iterator.getSampleDouble((int) x, (int) y, i);
            if (currentValue == Double.NaN) {
                continue;
            }
            if (currentValue > maxValue) {
                maxValue = currentValue;
                value = i;
            }
        }
        return value;
    }

    private double getBandOfMinValue(RandomIter iterator, long x, long y, int nBands) {
        double value = Double.NaN;
        double minValue = Double.MAX_VALUE;
        for (int i = 0; i < nBands; i++) {
            double currentValue = iterator.getSampleDouble((int) x, (int) y, i);
            if (currentValue == Double.NaN) {
                continue;
            }
            if (currentValue < minValue) {
                minValue = currentValue;
                value = i;
            }
        }
        return value;
    }

    private double getMaxCellValue(RandomIter iterator, long x, long y, int nBands) {
        double maxValue = Double.MIN_VALUE;
        for (int i = 0; i < nBands; i++) {
            double currentValue = iterator.getSampleDouble((int) x, (int) y, i);
            if (currentValue == Double.NaN) {
                continue;
            }
            if (currentValue > maxValue) {
                maxValue = currentValue;
            }
        }
        return maxValue == Double.MIN_VALUE ? Double.NaN : maxValue;
    }

    private double getMinCellValue(RandomIter iterator, long x, long y, int nBands) {
        double minValue = Double.MAX_VALUE;
        for (int i = 0; i < nBands; i++) {
            double currentValue = iterator.getSampleDouble((int) x, (int) y, i);
            if (currentValue == Double.NaN) {
                continue;
            }
            if (currentValue < minValue) {
                minValue = currentValue;
            }
        }
        return minValue == Double.MAX_VALUE ? Double.NaN : minValue;
    }

    private double getAvgCellValue(RandomIter iterator, long x, long y, int nBands) {
        int validBands = 0;
        double sum = 0.0;
        for (int i = 0; i < nBands; i++) {
            double currentValue = iterator.getSampleDouble((int) x, (int) y, i);
            if (Double.isNaN(currentValue)) {
                continue;
            }
            sum += currentValue;
            validBands++;
        }
        if (validBands == 0) {
            return Double.NaN;
        }
        return sum / validBands;
    }

    private double getSumCellValue(RandomIter iterator, long x, long y, int nBands) {
        double sum = 0.0;
        for (int i = 0; i < nBands; i++) {
            double currentValue = iterator.getSampleDouble((int) x, (int) y, i);
            if (Double.isNaN(currentValue)) {
                continue;
            }
            sum += currentValue;
        }
        return sum;
    }

    private Set<Double> getNodata(Resource resource, GridCoverage coverage, int band) {
        Set<Double> ret = new HashSet<>();
        if (resource.getParameters().contains("nodata")) {
            ret.add(resource.getParameters().get("nodata", Double.class));
        }
        return ret;
    }

    private CoordinateReferenceSystem getCrs(Geometry geometry) {
        var scale = Scale.create(geometry);
        var space = scale.getSpace();
        return ((ProjectionImpl) space.getProjection()).getCoordinateReferenceSystem();
    }

    private Interpolation getInterpolation(Metadata metadata) {

        String method = metadata.get(RasterAdapter.INTERPOLATION_PARAM, String.class);
        if (method != null) {
            switch (method) {
                case "bilinear" -> {
                    return new InterpolationBilinear();
                }
                case "nearest" -> {
                    return new InterpolationNearest();
                }
                case "bicubic" -> {
                    // TODO CHECK BITS
                    return new InterpolationBicubic(8);
                }
                case "bicubic2" -> {
                    // TODO CHECK BITS
                    return new InterpolationBicubic2(8);
                }
            }
        }
        return new InterpolationNearest();
    }

    private ReferencedEnvelope getEnvelope(Geometry geometry, CoordinateReferenceSystem crs) {
        var scale = Scale.create(geometry);
        var space = scale.getSpace();
        return ((EnvelopeImpl) space.getEnvelope()).getJTSEnvelope();
    }

    private GridGeometry2D getGridGeometry(Geometry geometry, ReferencedEnvelope envelope) {

        var space = geometry.dimension(Geometry.Dimension.Type.SPACE);
        if (space.getDimensionality() != 2 || !space.isRegular()) {
            throw new KlabInternalErrorException(
                    "raster encoder: cannot create grid for raster projection: shape is not a grid");
        }
        GeneralGridEnvelope gridRange =
                new GeneralGridEnvelope(
                        new int[]{0, 0},
                        new int[]{space.getShape().get(0).intValue(), (space.getShape().get(1).intValue())},
                        false);

        return new GridGeometry2D(gridRange, envelope);
    }

    /**
     * Coverages with caching. We keep a configurable total of coverages in memory using the session
     * cache, including their transformations indexed by geometry.
     *
     * @param resource
     * @return a coverage for the untransformed data. Never null
     */
    public GridCoverage getCoverage(Resource resource, Geometry geometry) {

        GridCoverage coverage = getOriginalCoverage(resource);

        // TODO if we have it in the cache for the principal file + space signature,
        // return that

        /*
         * build the needed Geotools context and the interpolation method
         */
        CoordinateReferenceSystem crs = getCrs(geometry);
        ReferencedEnvelope envelope = getEnvelope(geometry, crs);
        GridGeometry2D gridGeometry = getGridGeometry(geometry, envelope);
        Interpolation interpolation = getInterpolation(resource.getMetadata());

        /*
         * subset first
         */
        GridCoverage transformedCoverage = (GridCoverage) Operations.DEFAULT.resample(coverage, envelope, interpolation);

        /*
         * then resample
         */
        transformedCoverage = (GridCoverage) Operations.DEFAULT.resample(transformedCoverage, crs, gridGeometry, interpolation);

        return transformedCoverage;
    }

    private GridCoverage getOriginalCoverage(Resource resource) {

        File mainFile = null;
        for (var file : resource.getLocalFiles()) {
            if (RasterAdapter.fileExtensions.contains(Utils.Files.getFileExtension(file))) {
                if (file.exists() && file.canRead()) {
                    mainFile = file;
                    break;
                }
            }
        }

        if (mainFile == null) {
            throw new KlabResourceAccessException(
                    "raster resource " + resource.getUrn() + " cannot be accessed");
        }

        return readCoverage(mainFile);
    }

    public GridCoverage readCoverage(File mainFile) {

        GridCoverage2D ret = null;
        AbstractGridFormat format = GridFormatFinder.findFormat(mainFile);
        // this is a bit hackey but does make more geotiffs work
        Hints hints = new Hints();
        if (format instanceof GeoTiffFormat) {
            hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
        }
        GridCoverage2DReader reader = format.getReader(mainFile, hints);
        try {
            ret = reader.read(null);
        } catch (IOException e) {
            throw new KlabIOException(e);
        }

        // TODO caching?

        return ret;
    }
}
