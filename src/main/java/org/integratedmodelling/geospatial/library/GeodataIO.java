package org.integratedmodelling.geospatial.library;

import it.geosolutions.imageio.plugins.tiff.BaselineTIFFTagSet;
import java.awt.image.DataBuffer;
import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileWriter;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.integratedmodelling.geospatial.adapters.raster.*;
import org.integratedmodelling.geospatial.utils.Geotools;
import org.integratedmodelling.klab.api.collections.Pair;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Storage;
import org.integratedmodelling.klab.api.data.mediation.classification.DataKey;
import org.integratedmodelling.klab.api.exceptions.KlabIOException;
import org.integratedmodelling.klab.api.knowledge.KlabAsset;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.services.resources.adapters.Exporter;
import org.integratedmodelling.klab.api.services.runtime.extension.Library;
import org.integratedmodelling.klab.api.services.runtime.extension.Reference;
import org.integratedmodelling.klab.services.base.BaseService;
import org.integratedmodelling.klab.utilities.Utils;

@Library(
    name = "geospatial.io",
    description =
"""
GeoTIFF export for gridded spatial data. Not concerned with colormaps, viewports and the like - those
should be provided at the visualization side. Observations made with embedded WCS may already have a
connected Geotiff: if so, use that, otherwise produce and cache the output.
""")
public class GeodataIO {
  @Exporter(
      schema = "geotiff",
      geometry =
          "S2", // will be used on anything compatible with S2 (any other dimension must be size 1)
      fillCurve = Data.FillCurve.D2_XY, // force use of XY across the board, adapting as needed
      knowledgeClass = KlabAsset.KnowledgeClass.OBSERVATION,
      mediaType = "image/tiff;application=geotiff",
      description = "Export a numeric observation to a raw data GeoTiff")
  public InputStream exportGeotiffContinuous(
      Observation observation, Storage.DoubleScanner scanner, ContextScope scope) {
    try {
      var file = File.createTempFile("klab", ".tiff");
      exportObservation(file, observation, scanner, "tiff", scope);
      return new FileInputStream(file);
    } catch (IOException e) {
      scope.error(e);
      throw new KlabIOException(e);
    }
  }

  @Reference.Exporter(name = "geotiff") // TODO support this!
  public InputStream exportGeotiffCategorical(
      Observation observation, Storage.KeyScanner scanner, DataKey dataKey, ContextScope scope) {
    try {
      var file = File.createTempFile("klab", ".tiff");
      exportObservation(file, observation, scanner, "tiff", scope);
      return new FileInputStream(file);
    } catch (IOException e) {
      scope.error(e);
      throw new KlabIOException(e);
    }
  }

  /**
   * TODO provide another exporter that should be selected based on the request for a KeyScanner
   *
   * @param file
   * @param observation
   * @param scanner
   * @param format
   * @param scope
   * @return
   */
  public File exportObservation(
      File file,
      Observation observation,
      Storage.KeyScanner scanner,
      String format,
      ContextScope scope) {
    return null;
  }

  public File exportObservation(
      File file,
      Observation observation,
      Storage.DoubleScanner scanner,
      String format,
      ContextScope scope) {

    boolean addStyle = file.getName().endsWith(".zip");
    boolean samefolder =
        true; // TODO options.get(OPTION_DO_NOT_CREATE_INDIVIDUAL_FOLDERS, Boolean.FALSE);
    boolean doNotZip = true; // TODO options.get(OPTION_DO_NOT_ZIP_MULTIPLE_FILES, Boolean.FALSE)

    File dir = Utils.Files.changeExtension(file, "dir");
    File out = new File(dir, Utils.Files.getFileName(file));
    if (!samefolder) {
      dir.mkdirs();
    } else {
      dir.getAbsoluteFile().getParentFile().mkdirs();
      out = new File(dir.getAbsoluteFile().getParentFile(), Utils.Files.getFileName(file));
    }
    GridCoverage2D coverage = null;
    DataKey dataKey = scope.getDigitalTwin().getStorageManager().getStorage(observation).getKey();
    File outQml = Utils.Files.changeExtension(out, "qml");
    if (dataKey != null) {
      File outAux = Utils.Files.changeExtension(out, "tiff.aux.xml");
      File outCpg = Utils.Files.changeExtension(out, "tiff.vat.cpg");
      File outDbf = Utils.Files.changeExtension(out, "tiff.vat.dbf");
      try {
        // write categories aux xml TODO reintegrate
        //        writeAuxXml(outAux, dataKey);

        // write categories dbf
        writeAuxDbf(outDbf, dataKey);
        // TODO reintegrate
        //                        Utils.Files.writeStringToFile(outCpg, "UTF-8");
        //
        //                        // write QGIS style
        //                        writeQgisStyleCategories(outQml, state, locator);
      } catch (Exception e1) {
        // ignore, since the output still will be a valid tiff
        // THIS SHOULD BE LOGGED THOUGH
      }

      int noValue = -2147483648; // Integer.MAX_VALUE;
      // TODO reintegrate
      //      coverage = Geotools.stateToIntCoverage(scanner, noValue, null);
    } else {
      // write QGIS style TODO reintegrate
      //      try {
      //        writeQgisStyleContinuous(outQml, observation, scanner);
      //      } catch (Exception e) {
      //        // ignore, since the output still will be a valid tiff
      //        // THIS SHOULD BE LOGGED THOUGH
      //      }

      coverage =
          Geotools.stateToCoverage(
              observation, scanner, DataBuffer.TYPE_FLOAT, Float.NaN, scope, false);
    }

    if (format.equalsIgnoreCase("tiff")) {
      try {

        File rasterFile = Utils.Files.changeExtension(out, "tiff");
        GeoTiffWriter writer = new GeoTiffWriter(rasterFile);

        writer.setMetadataValue(
            Integer.toString(BaselineTIFFTagSet.TAG_SOFTWARE),
            "k.LAB (www.integratedmodelling.org)");

        writer.write(coverage, null);

        if (dir != null && addStyle) {
          if (!doNotZip) {
            File zip = Utils.Files.changeExtension(file, "zip");
            if (zip.exists()) {
              zip.delete();
            }
            Utils.Zip.zip(zip, dir, false, false);
            file = zip;
            org.apache.commons.io.FileUtils.deleteQuietly(dir);
          } else {
            file = dir;
          }
        } else {
          file = rasterFile;
        }
        return file;
      } catch (IOException e) {
        return null;
      }
    }

    return null;
  }

  //  private void writeAuxXml(File auxFile, DataKey dataKey) throws Exception {
  //
  //    RasterAuxXml rasterAuxXml = new RasterAuxXml();
  //    rasterAuxXml.rasterBand = new PAMDataset.PAMRasterBand();
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

  //  private void writeQgisStyleCategories(
  //      File qmlFile, Observation state, Storage.DoubleScanner scanner, ContextScope scope)
  //      throws Exception {
  //
  //    DataKey dataKey = scope.getDigitalTwin().getStorageManager().getStorage(state).getKey();
  //    Pair<RasterSymbolizer, String> rasterSymbolizerPair =
  //        Renderer.getRasterSymbolizer(state, scanner, scope);
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
  //            "<rasterrenderer band=\"1\" type=\"paletted\" alphaBand=\"-1\" opacity=\"1\"
  // nodataColor=\"\">\n");
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
  //    Utils.Files.writeStringToFile(qmlFile, sb.toString());
  //  }

  //  private void writeQgisStyleContinuous(
  //      File qmlFile, Observation state, Scheduler.Event locator, ContextScope scope)
  //      throws Exception {
  //
  //    var histogram = scope.getDigitalTwin().getStorageManager().getStorage(state).getHistogram();
  //    Colormap colorMap = null; // TODO stateSummary.getColormap();
  //    //        List<Double> range = stateSummary.getRange();
  //
  //    double min = histogram.getMin();
  //    double max = histogram.getMax();
  //
  //    List<String> labels = Arrays.asList("" + min, "" + max);
  //    List<String> colors = Arrays.asList("#FFFFFF", "#000000");
  //    if (colorMap != null) {
  //      // TODO
  //      //            labels = colorMap.getLabels();
  //      //            colors = colorMap.getColors();
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
  //    Utils.Files.writeStringToFile(qmlFile, sb.toString());
  //  }

  private boolean writeAuxDbf(File auxDbfFile, DataKey dataKey) throws Exception {

    DbaseFileHeader header = new DbaseFileHeader();
    header.addColumn("Value", 'N', 10, 0);
    int stringLimit = 100;
    header.addColumn("Label", 'C', stringLimit, 0);

    List<Pair<Integer, String>> values = dataKey.getAllValues();
    header.setNumRecords(values.size());

    try (FileOutputStream fout = new FileOutputStream(auxDbfFile)) {
      DbaseFileWriter dbf =
          new DbaseFileWriter(header, fout.getChannel(), Charset.forName("UTF-8"));
      for (Pair<Integer, String> pair : values) {
        Integer code = pair.getFirst();
        String classString = pair.getSecond();
        if (classString.length() > stringLimit) {
          classString = classString.substring(0, stringLimit);
        }
        dbf.write(new Object[] {code, classString});
      }
      dbf.close();
    }
    return true;
  }
}
