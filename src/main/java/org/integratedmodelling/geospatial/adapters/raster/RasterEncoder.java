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
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.util.*;

import org.eclipse.imagen.*;
import org.eclipse.imagen.iterator.RandomIter;
import org.eclipse.imagen.iterator.RandomIterFactory;
import org.geotools.api.coverage.grid.GridCoverage;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
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
import org.integratedmodelling.common.knowledge.GeometryRepository;
import org.integratedmodelling.geospatial.adapters.RasterAdapter;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.configuration.Configuration;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Storage;
import org.integratedmodelling.klab.api.exceptions.KlabIOException;
import org.integratedmodelling.klab.api.exceptions.KlabInternalErrorException;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.observation.scale.Scale;
import org.integratedmodelling.klab.api.services.runtime.Notification;
import org.integratedmodelling.klab.common.data.ExportFileCache;
import org.integratedmodelling.klab.runtime.scale.space.EnvelopeImpl;
import org.integratedmodelling.klab.runtime.scale.space.ProjectionImpl;
import org.integratedmodelling.klab.utilities.Utils;

/**
 * The {@code RasterEncoder} adapts a raster resource (file-based) to a passed geometry and builds
 * the correspondent Data buffer.
 */
public enum RasterEncoder {
  INSTANCE;

  // TODO substitute these with a more standardizable component option API
  private static final String OPTION_DO_NOT_CREATE_INDIVIDUAL_FOLDERS = "raster.individual.folders";
  private static final String OPTION_DO_NOT_ZIP_MULTIPLE_FILES =
      "raster.do.not.zip individual.files";
  private ExportFileCache cache;

  public ExportFileCache getCache() {
    if (cache == null) {
      cache =
          new ExportFileCache(
              Configuration.INSTANCE.getDataPath("klab.component.geospatial/cache"),
              "raster.cache",
              /* TODO configure */ 1024);
    }
    return cache;
  }

  /**
   * Take a Geotools coverage and do the rest. Separa""ted so other adapters with raster result
   * (WCS, STAC, OpenEO) can use it as is. Assumes that the fill curve is XY.
   *
   * @param resource
   * @param urnParameters
   * @param coverage
   * @param geometry
   * @param builder
   */
  public void encodeFromCoverage(
      Resource resource,
      Parameters<String> urnParameters,
      GridCoverage coverage,
      Geometry geometry,
      Data.Builder builder) {

    /*
     * Set the data from the transformed coverage
     */
    RenderedImage image = coverage.getRenderedImage();
    RandomIter iterator = RandomIterFactory.create(image, null);
    var scale = GeometryRepository.INSTANCE.scale(geometry);
    int band = 0;
    if (urnParameters.containsKey(RasterAdapter.BAND_PARAM)) {
      band = urnParameters.get(RasterAdapter.BAND_PARAM, Integer.class);
    } else {
      resource.getParameters().get(RasterAdapter.BAND_PARAM, 0);
    }
    int nBands = coverage.getNumSampleDimensions();
    Set<Double> nodata = getNodata(resource, coverage, band);
    GroovyShell shell = null;
    Binding binding = null;
    Script transformation = null;

    if (resource.getParameters().get(RasterAdapter.TRANSFORM_PARAM) != null
        && !resource
            .getParameters()
            .get(RasterAdapter.TRANSFORM_PARAM)
            .toString()
            .trim()
            .isEmpty()) {
      binding = new Binding();
      shell = new GroovyShell(binding);
      transformation =
          shell.parse(resource.getParameters().get(RasterAdapter.TRANSFORM_PARAM).toString());
    }

    BandMixing.Operation bandMixer = null;
    if (resource.getParameters().contains(RasterAdapter.BANDMIXER_PARAM)) {
      try {
        bandMixer =
            BandMixing.Operation.valueOf(
                resource.getParameters().get(RasterAdapter.BANDMIXER_PARAM, String.class));
      } catch (IllegalArgumentException e) {
        builder.notification(
            Notification.error(
                "Unsupported band mixing operation "
                    + resource.getParameters().get(RasterAdapter.BANDMIXER_PARAM, String.class)));
      }
    }

    var xy = scale.getSpace().getShape();
    var xx = xy.get(0);
    var yy = xy.get(1);
    var filler = builder.scanner(Storage.DoubleScanner.class);

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

        filler.add(value);
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

  private Interpolation getInterpolation(Parameters<String> metadata) {

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
            new int[] {0, 0},
            new int[] {space.getShape().get(0).intValue(), (space.getShape().get(1).intValue())},
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
    GridCoverage transformedCoverage =
        (GridCoverage) Operations.DEFAULT.resample(coverage, envelope, interpolation);

    /*
     * then resample
     */
    transformedCoverage =
        (GridCoverage)
            Operations.DEFAULT.resample(transformedCoverage, crs, gridGeometry, interpolation);

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

//  public boolean exportCoverage(
//      Observation observation,
//      Scheduler.Event locator,
//      GridCoverage coverage,
//      File file,
//      Parameters<String> options,
//      Scope scope) {
//
//    boolean addStyle = file.getName().endsWith(".zip");
//    var format = Utils.Files.getFileExtension(file);
//
//    boolean samefolder = options.get(OPTION_DO_NOT_CREATE_INDIVIDUAL_FOLDERS, Boolean.FALSE);
//
//    //      if (observation instanceof IState
//    //          && observation.getGeometry().getDimension(Type.SPACE) != null) {
//    //
//    //        if (observation.getScale().isSpatiallyDistributed()
//    //            && observation.getScale().getSpace().isRegular()) {
//    File dir = Utils.Files.changeExtension(file, "dir");
//    File out = new File(dir, Utils.Files.getFileName(file));
//    if (!samefolder) {
//      dir.mkdirs();
//    } else {
//      dir.getAbsoluteFile().getParentFile().mkdirs();
//      out = new File(dir.getAbsoluteFile().getParentFile(), Utils.Files.getFileName(file));
//    }
//    //          GridCoverage2D coverage;
//    //          IState state = (IState) observation;
//    var service =
//        scope.getService(
//            RuntimeService.class,
//            s -> s.serviceId().equals(observation.getContextualizationData().getServiceId()));
//    var dataKey = service.retrieveAsset(observation.getUrn(), locator, DataKey.class, scope);
//    var colormap = service.retrieveAsset(observation.getUrn(), locator, Colormap.class, scope);
//    File outQml = Utils.Files.changeExtension(out, "qml");
//    if (dataKey != null) {
//      File outAux = Utils.Files.changeExtension(out, "tiff.aux.xml");
//      File outCpg = Utils.Files.changeExtension(out, "tiff.vat.cpg");
//      File outDbf = Utils.Files.changeExtension(out, "tiff.vat.dbf");
//      try {
//        // write categories aux xml
//        writeAuxXml(outAux, dataKey);
//
//        // write categories dbf
//        writeAuxDbf(outDbf, dataKey);
//        FileUtils.writeStringToFile(outCpg, "UTF-8");
//
//        // write QGIS style
//        writeQgisStyleCategories(outQml, colormap, dataKey);
//      } catch (Exception e1) {
//        // ignore, since the output still will be a valid tiff
//        // THIS SHOULD BE LOGGED THOUGH
//      }
//
//      //            int noValue = -2147483648; // Integer.MAX_VALUE;
//      //            coverage =
//      //                GeotoolsUtils.INSTANCE.stateToIntCoverage(
//      //                    (IState) observation, locator, noValue, null);
//    } else {
//      // write QGIS style
//      try {
//        writeQgisStyleContinuous(outQml, observation, colormap, locator, scope);
//      } catch (Exception e) {
//        // ignore, since the output still will be a valid tiff
//        // THIS SHOULD BE LOGGED THOUGH
//      }
//
//      //            coverage =
//      //                GeotoolsUtils.INSTANCE.stateToCoverage(
//      //                    (IState) observation, locator, DataBuffer.TYPE_FLOAT, Float.NaN, true,
//      // null);
//    }
//
//    if (format.equalsIgnoreCase("tiff")) {
//      try {
//
//        File rasterFile = Utils.Files.changeExtension(out, "tiff");
//        GeoTiffWriter writer = new GeoTiffWriter(rasterFile);
//
//        writer.setMetadataValue(
//            Integer.toString(BaselineTIFFTagSet.TAG_SOFTWARE),
//            "k.LAB (www.integratedmodelling.org)");
//
//        writer.write(coverage, null);
//
//        if (dir != null && addStyle) {
//          if (!options.get(OPTION_DO_NOT_ZIP_MULTIPLE_FILES, Boolean.FALSE)) {
//            File zip = Utils.Files.changeExtension(file, "zip");
//            if (zip.exists()) {
//              FileUtils.deleteQuietly(zip);
//            }
//            Utils.Zip.zip(zip, dir, false, false);
//            file = zip;
//            FileUtils.deleteQuietly(dir);
//          } else {
//            file = dir;
//          }
//        } else {
//          file = rasterFile;
//        }
//        return true;
//        //              return file;
//      } catch (IOException e) {
//        return false;
//      }
//    }
//    //        }
//    //      }
//
//    return false;
//  }

//  private void writeAuxXml(File auxFile, DataKey dataKey) throws Exception {
//
//    RasterAuxXml rasterAuxXml = new RasterAuxXml();
//    rasterAuxXml.rasterBand = new PAMRasterBand();
//    rasterAuxXml.rasterBand.band = 1;
//    rasterAuxXml.rasterBand.attributeTable = new GDALRasterAttributeTable();
//
//    FieldDefn oidFieldDefn = new FieldDefn();
//    oidFieldDefn.index = 0;
//    oidFieldDefn.name = "OBJECTID";
//    oidFieldDefn.type = 0;
//    oidFieldDefn.usage = 0;
//    rasterAuxXml.rasterBand.attributeTable.fieldDefnList.add(oidFieldDefn);
//    FieldDefn valueFieldDefn = new FieldDefn();
//    valueFieldDefn.index = 1;
//    valueFieldDefn.name = "value";
//    valueFieldDefn.type = 0;
//    valueFieldDefn.usage = 0;
//    rasterAuxXml.rasterBand.attributeTable.fieldDefnList.add(valueFieldDefn);
//    FieldDefn labelFieldDefn = new FieldDefn();
//    labelFieldDefn.index = 2;
//    labelFieldDefn.name = "label";
//    labelFieldDefn.type = 2;
//    labelFieldDefn.usage = 0;
//    rasterAuxXml.rasterBand.attributeTable.fieldDefnList.add(labelFieldDefn);
//
//    List<Pair<Integer, String>> values = dataKey.getAllValues();
//    int index = 0;
//    for (Pair<Integer, String> pair : values) {
//      Integer code = pair.getFirst();
//      String classString = pair.getSecond();
//      Row row = new Row();
//      row.index = index;
//      row.fList.add(String.valueOf(index));
//      row.fList.add(code.toString());
//      row.fList.add(classString);
//      rasterAuxXml.rasterBand.attributeTable.rowList.add(row);
//      index++;
//    }
//
//    JAXBContext context = JAXBContext.newInstance(RasterAuxXml.class);
//    Marshaller marshaller = context.createMarshaller();
//    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
//    // StringWriter stringWriter = new StringWriter();
//    marshaller.marshal(rasterAuxXml, auxFile);
//    // System.out.println(stringWriter.toString());
//  }
//
//  private void writeQgisStyleCategories(File qmlFile, Colormap colormap, DataKey dataKey)
//      throws Exception {
//
//    Pair<RasterSymbolizer, String> rasterSymbolizerPair =
//        Renderer.INSTANCE.getRasterSymbolizer(colormap, dataKey);
//    RasterSymbolizer rasterSymbolizer = rasterSymbolizerPair.getFirst();
//    ColorMap colorMap = rasterSymbolizer.getColorMap();
//    ColorMapEntry[] colorMapEntries = colorMap.getColorMapEntries();
//    HashMap<String, String> label2ColorMap = new HashMap<>();
//    for (ColorMapEntry colorMapEntry : colorMapEntries) {
//      String label = colorMapEntry.getLabel();
//      String color = colorMapEntry.getColor().evaluate(null, String.class);
//      label2ColorMap.put(label, color);
//    }
//
//    String ind = "\t";
//    StringBuilder sb = new StringBuilder();
//    sb.append("<qgis>\n");
//    sb.append(ind).append("<pipe>\n");
//    sb.append(ind)
//        .append(ind)
//        .append(
//            "<rasterrenderer band=\"1\" type=\"paletted\" alphaBand=\"-1\" opacity=\"1\" nodataColor=\"\">\n");
//    sb.append(ind).append(ind).append(ind).append("<colorPalette>\n");
//    List<Pair<Integer, String>> values = dataKey.getAllValues();
//    for (Pair<Integer, String> pair : values) {
//      sb.append(ind).append(ind).append(ind).append(ind);
//
//      Integer code = pair.getFirst();
//      String classString = pair.getSecond();
//      String color = label2ColorMap.get(classString);
//
//      // <paletteEntry value="0" alpha="255" color="#7e7fef" label="cat0"/>
//      sb.append(
//          "<paletteEntry value=\""
//              + code
//              + "\" alpha=\"255\" color=\""
//              + color
//              + "\" label=\""
//              + classString
//              + "\"/>\n");
//    }
//    sb.append(ind).append(ind).append(ind).append("</colorPalette>\n");
//    sb.append(ind).append(ind).append("</rasterrenderer>\n");
//    sb.append(ind).append("</pipe>\n");
//    sb.append("</qgis>\n");
//
//    FileUtils.writeStringToFile(qmlFile, sb.toString());
//  }
//
//  private void writeQgisStyleContinuous(
//      File qmlFile,
//      Observation observation,
//      Colormap colormap,
//      Scheduler.Event locator,
//      Scope scope)
//      throws Exception {
//
//    var service =
//        scope.getService(
//            RuntimeService.class,
//            s -> s.serviceId().equals(observation.getContextualizationData().getServiceId()));
//    Histogram histogram =
//        service.retrieveAsset(observation.getUrn(), locator, Histogram.class, scope);
//
//    double min = histogram.getMin();
//    double max = histogram.getMax();
//
//    List<String> labels = Arrays.asList("" + min, "" + max);
//    List<String> colors = Arrays.asList("#FFFFFF", "#000000");
//    if (colormap != null) {
//      labels = colormap.getEntries().stream().map(Colormap.Entry::getLabel).toList();
//      colors = colormap.getEntries().stream().map(Colormap.Entry::getColor).toList();
//    }
//
//    String ind = "\t";
//    StringBuilder sb = new StringBuilder();
//    sb.append("<qgis>\n");
//    sb.append(ind).append("<pipe>\n");
//    sb.append(ind)
//        .append(ind)
//        .append("<rasterrenderer band=\"1\" type=\"singlebandpseudocolor\"")
//        .append(" classificationMax=\"")
//        .append(max)
//        .append("\"")
//        .append(" classificationMin=\"")
//        .append(min)
//        .append("\"")
//        .append(" alphaBand=\"-1\" opacity=\"1\" nodataColor=\"\">\n");
//
//    sb.append(ind).append(ind).append(ind).append("<rastershader>\n");
//    sb.append(ind)
//        .append(ind)
//        .append(ind)
//        .append(ind)
//        .append("<colorrampshader ")
//        .append(" minimumValue=\"")
//        .append(min)
//        .append("\"")
//        .append(" maximumValue=\"")
//        .append(max)
//        .append("\"")
//        .append(" colorRampType=\"INTERPOLATED\"")
//        .append(" classificationMode=\"1\"")
//        .append(" clip=\"0\"")
//        .append(">\n");
//    for (int i = 0; i < labels.size(); i++) {
//      sb.append(ind).append(ind).append(ind).append(ind).append(ind);
//
//      String label = labels.get(i);
//      String color = colors.get(i);
//
//      // <item color="#d7191c" value="846.487670898438" label="846,4877" alpha="255"/>
//      sb.append(
//          "<item color=\""
//              + color
//              + "\" value=\""
//              + label
//              + "\" label=\""
//              + label
//              + "\" alpha=\"255\"/>\n");
//    }
//    sb.append(ind).append(ind).append(ind).append(ind).append("</colorrampshader>\n");
//    sb.append(ind).append(ind).append(ind).append("</rastershader>\n");
//
//    sb.append(ind).append(ind).append("</rasterrenderer>\n");
//    sb.append(ind).append("</pipe>\n");
//    sb.append("</qgis>\n");
//
//    Utils.Files.writeStringToFile(sb.toString(), qmlFile);
//  }
//
//  private boolean writeAuxDbf(File auxDbfFile, DataKey dataKey) throws Exception {
//
//    DbaseFileHeader header = new DbaseFileHeader();
//    header.addColumn("Value", 'N', 10, 0);
//    int stringLimit = 100;
//    header.addColumn("Label", 'C', stringLimit, 0);
//
//    List<Pair<Integer, String>> values = dataKey.getAllValues();
//    header.setNumRecords(values.size());
//
//    try (FileOutputStream fout = new FileOutputStream(auxDbfFile)) {
//      DbaseFileWriter dbf = new DbaseFileWriter(header, fout.getChannel(), StandardCharsets.UTF_8);
//      for (Pair<Integer, String> pair : values) {
//        Integer code = pair.getFirst();
//        String classString = pair.getSecond();
//        if (classString.length() > stringLimit) {
//          classString = classString.substring(0, stringLimit);
//        }
//        dbf.write(new Object[] {code, classString});
//      }
//      dbf.close();
//    }
//    return true;
//  }
}
