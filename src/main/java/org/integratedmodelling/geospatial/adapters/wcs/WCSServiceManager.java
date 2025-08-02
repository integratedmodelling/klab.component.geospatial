package org.integratedmodelling.geospatial.adapters.wcs;

import com.github.underscore.lodash.U;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.jxpath.JXPathContext;
import org.integratedmodelling.common.authentication.Authentication;
import org.integratedmodelling.common.authentication.Authorization;
import org.integratedmodelling.common.logging.Logging;
import org.integratedmodelling.geospatial.adapters.RasterAdapter;
import org.integratedmodelling.klab.api.authentication.ExternalAuthenticationCredentials;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.data.mediation.NumericRange;
import org.integratedmodelling.klab.api.exceptions.KlabAuthorizationException;
import org.integratedmodelling.klab.api.exceptions.KlabInternalErrorException;
import org.integratedmodelling.klab.api.exceptions.KlabUnimplementedException;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.geometry.impl.GeometryBuilder;
import org.integratedmodelling.klab.api.geometry.impl.GeometryImpl;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Envelope;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Projection;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.TemporalExtension;
import org.integratedmodelling.klab.configuration.ServiceConfiguration;
import org.integratedmodelling.klab.runtime.scale.space.ProjectionImpl;
import org.integratedmodelling.klab.utilities.Utils;

public class WCSServiceManager {

  public static final String WGS84_BOUNDING_BOX = "ows:WGS84BoundingBox";
  public static final String IDENTIFIER = "ows:Identifier";
  public static final String CRS = "crs";
  public static final String INFINITY = "Infinity";
  public static final String NULL_VALUE = "NullValue";
  public static final String LOWER_CORNER = "ows:LowerCorner";
  public static final String UPPER_CORNER = "ows:UpperCorner";
  public static final String SUPPORTED_CRS = "ows:SupportedCRS";
  public static final String COVERAGE_ID = "wcs:CoverageId";
  public static final String RANGE = "ows:Range";
  public static final String RANGE_TYPE = "gmlcov:rangeType";

  /** Default time of expiration of layer information is 2h */
  private long layerInfoExpirationMilliseconds = 120 * 60 * 1000;

  /**
   * If true (default), double underscores (__) in layer identifiers are translated into namespace
   * separator (:) before retrieval of the layer identifier. This accommodates Geoserver which uses
   * namespaced IDs.
   */
  private boolean translateDoubleUnderscoreToNamespaceSeparator = true;

  private List<Throwable> errors = new ArrayList<>();
  private Map<String, WCSLayer> layers = Collections.synchronizedMap(new HashMap<>());
  // all identifiers, also when the layers are not there
  private Set<String> identifiers = Collections.synchronizedSet(new HashSet<>());
  private String serviceUrl;
  private Version version;
  private Authorization authorization = null;
  /*
  Using a cache for WCS responses.
  May use a more sophisticated strategy later. Should have a timeout per entry and a maintenance thread.
  See https://www.javacodegeeks.com/2013/12/extending-guava-caches-to-overflow-to-disk.html
   */
  private Map<String, String> wcsCache = new HashMap<>();

  //      FileSystemCacheBuilder.newBuilder().maximumSize(300L).softValues().build();

  /** TODO Unirest should be removed and normal HttpClient (or Utils.Http.Client) should be used. */
  static {
    // Note: SSL verification is disabled for compatibility with some WCS services
    // In production should probably enable SSL verification and properly
    // configure trusted certificates, or provide a parameter to configure the service
    // explicitly.
    Unirest.config().verifySsl(false);
  }

  /**
   * Gets the expiration time for layer information in milliseconds.
   *
   * @return the expiration time in milliseconds
   */
  public long getLayerInfoExpirationMilliseconds() {
    return layerInfoExpirationMilliseconds;
  }

  /**
   * Sets the expiration time for layer information in milliseconds.
   *
   * @param milliseconds the expiration time in milliseconds
   */
  public void setLayerInfoExpirationMilliseconds(long milliseconds) {
    this.layerInfoExpirationMilliseconds = milliseconds;
  }

  /**
   * Checks if double underscores in layer identifiers should be translated to namespace separators.
   *
   * @return true if translation is enabled, false otherwise
   */
  public boolean isTranslateDoubleUnderscoreToNamespaceSeparator() {
    return translateDoubleUnderscoreToNamespaceSeparator;
  }

  /**
   * Sets whether double underscores in layer identifiers should be translated to namespace
   * separators.
   *
   * @param translate true to enable translation, false to disable
   */
  public void setTranslateDoubleUnderscoreToNamespaceSeparator(boolean translate) {
    this.translateDoubleUnderscoreToNamespaceSeparator = translate;
  }

  public boolean hasErrors() {
    return errors.size() > 0;
  }

  public Collection<WCSLayer> getLayers() {
    return layers.values();
  }

  public WCSLayer getLayer(String id) {
    return layers.get(id);
  }

  public String getServiceUrl() {
    return serviceUrl;
  }

  public Version getServiceVersion() {
    return version;
  }

  public class WCSLayer {

    class Band {
      public Band(Map<?, ?> data) {
        if (data.containsKey("field")) {
          data = (Map<?, ?>) data.get("field");
        }
        this.name = data.get("-name").toString();
        Map<?, ?> quantity = (Map<?, ?>) data.get("swe:Quantity");
        if (quantity.containsKey("swe:constraint")) {

          String interval =
              Utils.Maps.get(
                  quantity, "swe:constraint/swe:AllowedValues/swe:interval", String.class);

          double[] nils = Utils.Numbers.doubleArrayFromString(interval, "\\s+");
          if (Utils.Numbers.equal(nils[0], nils[1])) {
            // it's the nodata value - but it's supposed to be the allowed interval.
            this.nodata.add(nils[0]);
          } else {
            this.boundaries = NumericRange.create(nils[0], nils[1], false, false);
          }

          if (quantity.containsKey("swe:nilValues")) {

            String nilval =
                Utils.Maps.get(
                    quantity, "swe:nilValues/swe:NilValues/swe:nilValue/#text", String.class);
            if (nilval == null) {
              Logging.INSTANCE.warn(
                  "WCS: null key in nilValues is not null anymore: revise Geotools API versions");
            } else {
              this.nodata.add(Double.parseDouble(nilval));
            }
          }
        }
      }

      String name;
      Set<Double> nodata = new HashSet<>();
      NumericRange boundaries;
    }

    // identifier from capabilities (simple, no namespace)
    private String name;
    // identifier from describeCoverage, to use for retrieval (includes namespace in
    // Geoserver)
    private String identifier;
    // envelope in WGS84 from capabilities
    private Envelope wgs84envelope;
    // if this is empty, we don't know from the server and we should just try,
    // signaling an error
    private Set<Projection> supportedProjections = new HashSet<>();

    // if for any reason we can't parse these, they will be set to the WSG84 from
    // the capabilities
    private Envelope originalEnvelope;
    private Projection originalProjection;
    // this takes over band info when bands are unspecified
    private Set<Double> nodata = new HashSet<>();
    // this may not be filled in despite the existence of at least one band
    private List<Band> bands = new ArrayList<>();

    // set to true when a getCoverage response has been parsed
    private boolean finished = false;
    private String message = "";
    private boolean error = false;
    private int[] gridShape;
    private long timestamp = System.currentTimeMillis();
    private boolean skipRefresh;
    private TemporalExtension temporalExtension;

    public WCSLayer(boolean skipRefresh) {
      this.skipRefresh = skipRefresh;
    }

    public String getName() {
      return name;
    }

    public String getIdentifier() {
      describeCoverage();
      return identifier;
    }

    /**
     * Return the same as {@link #getIdentifier()} but if there are service-specific transformations
     * to adapt it for a request, perform them first.
     *
     * @return the request-ready identifier
     */
    public String getRequestIdentifier() {
      describeCoverage();
      return translateDoubleUnderscoreToNamespaceSeparator
          ? identifier.replaceAll("__", ":")
          : identifier;
    }

    public URL buildRetrieveUrl(
            Version version,
            Geometry geometry,
            RasterAdapter.Interpolation interpolation) {
      return WCSServiceManager.this.buildRetrieveUrl(this, version, geometry, interpolation);
    }

    public Envelope getWgs84envelope() {
      return wgs84envelope;
    }

    public Set<Projection> getSupportedProjections() {
      describeCoverage();
      return supportedProjections;
    }

    public Envelope getOriginalEnvelope() {
      describeCoverage();
      return originalEnvelope;
    }

    public Projection getOriginalProjection() {
      describeCoverage();
      return originalProjection;
    }

    public Set<Double> getNodata(int band) {
      describeCoverage();
      return bands.size() > band ? bands.get(band).nodata : nodata;
    }

    public boolean isError() {
      describeCoverage();
      return error || identifier == null;
    }

    /**
     * Retrieves and parses coverage description from the WCS service. This method is called by
     * getter methods to ensure the layer information is up-to-date.
     */
    private void describeCoverage() {
      // Check if we need to refresh the coverage information
      if (!finished || (System.currentTimeMillis() - timestamp) > layerInfoExpirationMilliseconds) {
        finished = true;
        timestamp = System.currentTimeMillis();

        try {
          // Build the URL for describeCoverage request
          URL url =
              new URL(
                  serviceUrl
                      + "?service=WCS&version="
                      + version
                      + "&request=DescribeCoverage&"
                      + (version.getMajor() >= 2 ? "coverageId=" : "identifiers=")
                      + name);

          Map<?, ?> coverage = null;

          // Check if we have a cached response
          String cached = wcsCache.get /*IfPresent*/(url.toString());
          if (cached != null) {
            Map<?, ?> map = Utils.Json.parseObject(cached, Map.class);
            coverage = map;
            Logging.INSTANCE.debug("Using cached coverage description for " + name);
          }

          // If not in cache, fetch from service
          if (coverage == null) {
            try (InputStream input = url.openStream()) {
              String content = IOUtils.toString(input, StandardCharsets.UTF_8);
              coverage = (Map<?, ?>) U.fromXmlMap(content);
              Map<Object, Object> tocache = new HashMap<>(coverage);
              tocache.put("timestamp", System.currentTimeMillis());
              wcsCache.put(url.toString(), Utils.Json.asString(tocache));
              Logging.INSTANCE.debug("Retrieved coverage description for " + name);
            } catch (IOException e) {
              error = true;
              message = "Failed to retrieve coverage description: " + e.getMessage();
              Logging.INSTANCE.error(
                  "Error retrieving coverage description for " + name + ": " + e.getMessage(), e);
            }
          }

          // Parse the coverage information
          if (coverage != null) {
            if (version.getMajor() >= 2) {
              parseV2(coverage);
            } else {
              parseV1(coverage);
            }
          }

        } catch (Throwable t) {
          this.error = true;
          this.message = "Error processing coverage description: " + t.getMessage();
          Logging.INSTANCE.error(
              "Error processing coverage description for " + name + ": " + t.getMessage(), t);
        }
      }
    }

    /**
     * Parses coverage description from WCS 1.x service.
     *
     * @param coverage the coverage description map
     */
    private void parseV1(Map<?, ?> coverage) {
      try {
        // Extract identifier
        this.identifier = coverage.get(IDENTIFIER).toString();

        // Extract supported projections
        JXPathContext context = JXPathContext.newContext(coverage);
        if (coverage.get(SUPPORTED_CRS) instanceof Collection) {
          for (Object crs : ((Collection<?>) coverage.get(SUPPORTED_CRS))) {
            Projection projection = Projection.of(crs.toString());
            this.supportedProjections.add(projection);
          }
        }

        // Default to WGS84 envelope and projection
        this.originalEnvelope = wgs84envelope;
        this.originalProjection = Projection.getLatLon();

        // Extract bounding box information
        for (Iterator<?> it = context.iterate("Domain/BoundingBox"); it.hasNext(); ) {
          Map<?, ?> bbox = (Map<?, ?>) it.next();

          // Ignore EPSG::4326 which has swapped coordinates, and let other specs override the
          // defaults
          if (bbox.get(CRS) instanceof String
              && !bbox.get(CRS).equals("urn:ogc:def:crs:EPSG::4326")) {
            this.originalProjection = Projection.of(bbox.get(CRS).toString());
            double[] upperCorner =
                Utils.Numbers.doubleArrayFromString(
                    ((Map<?, ?>) bbox).get(UPPER_CORNER).toString(), "\\s+");
            double[] lowerCorner =
                Utils.Numbers.doubleArrayFromString(
                    ((Map<?, ?>) bbox).get(LOWER_CORNER).toString(), "\\s+");
            this.originalEnvelope =
                Envelope.of(
                    lowerCorner[0],
                    upperCorner[0],
                    lowerCorner[1],
                    upperCorner[1],
                    Projection.getLatLon());
          }
        }

        // Extract range information (nodata values)
        if (coverage.get(RANGE) instanceof Map) {
          Map<?, ?> range = (Map<?, ?>) coverage.get(RANGE);
          if (range.containsKey(NULL_VALUE)
              && !range.get(NULL_VALUE).toString().contains(INFINITY)) {
            // Add nodata value to the global nodata set
            this.nodata.add(Double.parseDouble(range.get(NULL_VALUE).toString()));
          }

          // Note: Additional information that could be extracted in future:
          // - Interpolation methods and defaults
          // - Axis information for bands
          // - Domain/GridCRS for grid shape
          // - SupportedFormat
          // - Keywords for URN metadata
        }
      } catch (Exception e) {
        this.error = true;
        this.message = "Error parsing WCS 1.x coverage description: " + e.getMessage();
        Logging.INSTANCE.error(
            "Error parsing WCS 1.x coverage for " + name + ": " + e.getMessage(), e);
      }
    }

    /**
     * Parses coverage description from WCS 2.x service.
     *
     * @param cov the coverage description map
     */
    private void parseV2(Map<?, ?> cov) {
      try {
        // Default to name and WGS84 envelope/projection
        this.identifier = this.name;
        this.originalEnvelope = this.wgs84envelope;
        this.originalProjection = Projection.getLatLon();

        // Extract coverage description
        Map<?, ?> coverage =
            Utils.Maps.get(cov, "wcs:CoverageDescriptions/wcs:CoverageDescription", Map.class);

        // Get coverage ID if available
        if (Utils.Maps.get(coverage, COVERAGE_ID, String.class) instanceof String) {
          this.identifier = coverage.get(COVERAGE_ID).toString();
        }

        // Extract bounding box information
        Map<?, ?> bounds = (Map<?, ?>) coverage.get("gml:boundedBy");
        this.originalProjection =
            Projection.of(Utils.Maps.get(bounds, "gml:Envelope/-srsName", String.class));
        double[] upperCorner =
            Utils.Numbers.doubleArrayFromString(
                Utils.Maps.get(bounds, "gml:Envelope/gml:upperCorner", String.class), "\\s+");
        double[] lowerCorner =
            Utils.Numbers.doubleArrayFromString(
                Utils.Maps.get(bounds, "gml:Envelope/gml:lowerCorner", String.class), "\\s+");
        this.originalEnvelope =
            Envelope.of(
                lowerCorner[0],
                upperCorner[0],
                lowerCorner[1],
                upperCorner[1],
                (Projection) this.originalProjection);

        // Extract band information from rangeType
        Map<?, ?> rangeType = Utils.Maps.get(coverage, RANGE_TYPE, Map.class);
        if (rangeType instanceof Map && !rangeType.isEmpty()) {
          processBandInformation(rangeType);
        }

        // Extract grid shape information from domainSet
        Map<?, ?> domain =
            Utils.Maps.get(
                coverage, "gml:domainSet/gml:RectifiedGrid/gml:limits/gml:GridEnvelope", Map.class);
        int[] gridHighRange =
            Utils.Numbers.intArrayFromString(domain.get("gml:high").toString(), "\\s+");
        int[] gridLowRange =
            Utils.Numbers.intArrayFromString(domain.get("gml:low").toString(), "\\s+");
        this.gridShape =
            new int[] {gridHighRange[0] - gridLowRange[0], gridHighRange[1] - gridLowRange[1]};

        // Note: If the projection flips coordinates, we might need to swap grid dimensions
        // This is currently disabled but might be needed in some cases:
        // if (this.originalProjection.flipsCoordinates()) {
        //   this.gridShape = new int[] { this.gridShape[1], this.gridShape[0] };
        // }

      } catch (Throwable t) {
        this.error = true;
        this.message = "Error parsing WCS 2.x coverage description: " + t.getMessage();
        Logging.INSTANCE.error(
            "Error parsing WCS 2.x coverage for " + name + ": " + t.getMessage(), t);
      }
    }

    /**
     * Helper method to process band information from the range type.
     *
     * @param rangeType the range type map containing band information
     */
    private void processBandInformation(Map<?, ?> rangeType) {
      Object fields = Utils.Maps.get(rangeType, "swe:DataRecord/swe:field", Object.class);

      if (fields instanceof Map) {
        // Handle single field or field container
        if (((Map<?, ?>) fields).containsKey("field")) {
          List<?> bandefs = (List<?>) ((Map<?, ?>) fields).get("field");
          for (Object o : bandefs) {
            bands.add(new Band((Map<?, ?>) o));
          }
        } else if (((Map<?, ?>) fields).containsKey("-name")) {
          bands.add(new Band((Map<?, ?>) fields));
        }
      } else if (fields instanceof List) {
        // Handle list of fields
        for (Object o : (List<?>) fields) {
          if (o instanceof Map) {
            if (((Map<?, ?>) o).containsKey("field")) {
              List<?> bandefs = (List<?>) ((Map<?, ?>) o).get("field");
              for (Object fo : bandefs) {
                bands.add(new Band((Map<?, ?>) fo));
              }
            } else if (((Map<?, ?>) o).containsKey("-name")) {
              bands.add(new Band((Map<?, ?>) o));
            }
          }
        }
      }
    }

    @Override
    public String toString() {
      return (name == null ? "NULL NAME" : name)
          + " "
          + (originalEnvelope == null ? "NO ENVELOPE" : originalEnvelope.asShape().encode())
          + "\n   "
          + (getGeometry() == null ? "NO GEOMETRY" : getGeometry().encode());
    }

    /**
     * Build and return the geometry for the layer. If the layer comes from WCS 1.x it won't have a
     * grid shape. The envelope comes in the original projection unless that flips coordinates, in
     * which case EPSG:4326 is used.
     *
     * @return the geometry.
     */
    public Geometry getGeometry() {

      var gBuilder = new GeometryBuilder();
      var builder =
          gBuilder
              .space()
              .regular()
              .boundingBox(
                  wgs84envelope.getMinX(),
                  wgs84envelope.getMaxX(),
                  wgs84envelope.getMinY(),
                  wgs84envelope.getMaxY());

      if (gridShape != null) {
        builder = builder.size((long) gridShape[0], (long) gridShape[1]);
      }

      if (originalProjection != null && originalEnvelope != null) {
        if (originalProjection.flipsCoordinates()) {
          // use the WGS84
          builder = builder.projection(Projection.DEFAULT_PROJECTION_CODE);
        } else {
          builder = builder.projection(originalProjection.getCode());
        }
      } else if (wgs84envelope != null) {
        builder = builder.projection(Projection.DEFAULT_PROJECTION_CODE);
      }

      if (this.temporalExtension != null) {
        gBuilder
            .time()
            .start(this.temporalExtension.getStart())
            .end(this.temporalExtension.getEnd())
            .extension(this.temporalExtension.getTimestamps());
      }

      return gBuilder.build();
    }

    /**
     * Returns the temporal coverage of this layer. This will return null unless the layer has EO
     * extensions, which is supported only with mosaic and NetCDF layers in geoserver.
     *
     * @return the temporal extension of this layer, or null if not available
     */
    public TemporalExtension getTemporalCoverage() {
      return this.temporalExtension;
    }

    public String getMessage() {
      return this.message;
    }
  }

  /**
   * Test method for WCSManager functionality.
   *
   * @param args command line arguments (not used)
   */
  public static void main(String[] args) {
    // Example: Get layer names from capabilities, then use describeCoverage on all layers

    // This installs the geospatial object instantiators to make the static of() work
    // in envelopes, projections etc.
    ServiceConfiguration.injectInstantiators();

    WCSServiceManager service =
        new WCSServiceManager(
            "https://integratedmodelling.org/geoserver/ows", Version.create("2.0.1"));

    //    WCSManager service =
    //        new WCSManager("https://www.geo.euskadi.eus/WCS_KARTOGRAFIA",
    // Version.create("1.0.0"));
    for (WCSLayer layer : service.getLayers()) {
      System.out.println("Found layer: " + layer.getName());
      //      // The service expects a URL like:
      //      //
      // https://www.geo.euskadi.eus/geoeuskadi/services/U11/WCS_KARTOGRAFIA/MapServer/WCSServer?SERVICE=WCS&VERSION=1.0.0&REQUEST=DescribeCoverage&COVERAGE=1
      //      layer.describeCoverage();
      //      System.out.println(layer);
    }
  }

  /**
   * Creates a new WCSManager for the specified service URL and version.
   *
   * @param serviceUrl the URL of the WCS service
   * @param version the version of the WCS service
   */
  @SuppressWarnings("unchecked")
  public WCSServiceManager(String serviceUrl, Version version) {
    this.serviceUrl = serviceUrl;
    this.version = version;

    // Handle authentication if credentials are available
    ExternalAuthenticationCredentials credentials =
        Authentication.INSTANCE.getCredentials(serviceUrl, null);
    if (credentials != null) {
      this.authorization = new Authorization(credentials);
      if (!this.authorization.isOnline()) {
        throw new KlabAuthorizationException(
            "authorization credentials for " + serviceUrl + " rejected");
      }
    }

    try {
      // Build the URL for getCapabilities request
      String url = serviceUrl + "?service=WCS&request=getCapabilities&version=" + version;

      // Make HTTP request to get capabilities
      HttpResponse<String> response = Unirest.get(url).asString();

      if (response.isSuccess()) {
        String content = response.getBody();

        // Hash the content to check if it has changed since last request
        String hash = Utils.Strings.hash(content);
        String prev = wcsCache.get /*IfPresent*/(url);
        boolean skipRefresh = (prev != null && hash.equals(prev));

        if (skipRefresh) {
          Logging.INSTANCE.info(
              "WCS catalog at " + url + " is unchanged since last read: coverage cache is valid");
        } else {
          Logging.INSTANCE.info(
              "WCS catalog at "
                  + url
                  + " has changed since last read: coverage cache expires in 12h");
        }

        wcsCache.put(url.toString(), hash);

        // Parse the XML content
        Map<?, ?> capabilitiesType = (Map<?, ?>) U.fromXmlMap(content);

        if (version.getMajor() == 1) {
          // Process WCS 1.x capabilities
          // Get coverage offerings from WCS 1.x capabilities
          for (Object o :
              Utils.Maps.get(
                  capabilitiesType,
                  "WCS_Capabilities/ContentMetadata/CoverageOfferingBrief",
                  List.class)) {
            Map<String, Object> item = (Map<String, Object>) o;
            Object name = item.get("name");
            Object label = item.get("label");
            if (name != null) {
              identifiers.add(name.toString());
            }
          }

          // Build the layers individually by requesting describeCoverage for each identifier
          for (String identifier : identifiers) {
            String sburl =
                serviceUrl
                    + "?SERVICE=WCS&VERSION="
                    + version
                    + "&REQUEST=DescribeCoverage&COVERAGE="
                    + identifier;

            response = Unirest.get(sburl).asString();
            if (response.isSuccess()) {
              Map<?, ?> layer = U.fromXmlMap(response.getBody());
              Map<?, ?> offering =
                  Utils.Maps.get(layer, "CoverageDescription/CoverageOffering", Map.class);
              // ...and then throw it away????

              System.out.println("Retrieved layer description: " + offering);

            } else {
              Logging.INSTANCE.warn("Failed to retrieve layer description for: " + identifier);
            }
          }

        } else {
          // Process WCS 2.x capabilities
          // Get coverage summaries from WCS 2.x capabilities
          for (Object o :
              Utils.Maps.get(
                  capabilitiesType,
                  "wcs:Capabilities/wcs:Contents/wcs:CoverageSummary",
                  Collection.class)) {
            Map<String, Object> item = (Map<String, Object>) o;
            Object name = item.get(version.getMajor() >= 2 ? COVERAGE_ID : IDENTIFIER);
            if (name != null) {
              identifiers.add(name.toString());
            }
          }

          for (Object o :
              Utils.Maps.get(
                  capabilitiesType,
                  "wcs:Capabilities/wcs:Contents/wcs:CoverageSummary",
                  Collection.class)) {

            Map<String, Object> item = (Map<String, Object>) o;

            Object name = item.get(version.getMajor() >= 2 ? COVERAGE_ID : IDENTIFIER);
            Object bbox = item.get(WGS84_BOUNDING_BOX);

            if (name instanceof String && bbox instanceof Map) {

              WCSLayer layer = new WCSLayer(skipRefresh);

              layer.name = name.toString();
              double[] upperCorner =
                  Utils.Numbers.doubleArrayFromString(
                      ((Map<?, ?>) bbox).get(UPPER_CORNER).toString(), "\\s+");
              double[] lowerCorner =
                  Utils.Numbers.doubleArrayFromString(
                      ((Map<?, ?>) bbox).get(LOWER_CORNER).toString(), "\\s+");
              layer.wgs84envelope =
                  Envelope.of(
                      lowerCorner[0],
                      upperCorner[0],
                      lowerCorner[1],
                      upperCorner[1],
                      Projection.getLatLon());

              layers.put(layer.name, layer);
            }
          }
        }
      } else {
        String errorMessage =
            "Cannot access content at "
                + url
                + ": "
                + response.getStatus()
                + " "
                + response.getStatusText();
        Logging.INSTANCE.error(errorMessage);
        errors.add(new KlabInternalErrorException(errorMessage));
      }

    } catch (Throwable e) {
      errors.add(e);
      Logging.INSTANCE.error(
          "Error initializing WCS service at " + serviceUrl + ": " + e.getMessage(), e);
    }
  }

  public URL buildRetrieveUrl(
      WCSLayer layer,
      Version version,
      Geometry geometry,
      RasterAdapter.Interpolation interpolation) {

    var space = geometry.dimension(Geometry.Dimension.Type.SPACE);
    URL url = null;

    if (space.getShape().size() != 2 || !space.isRegular()) {
      throw new IllegalArgumentException(
          "cannot retrieve  a grid dataset from WCS in a non-grid context");
    }

    String rcrs = space.getParameters().get(GeometryImpl.PARAMETER_SPACE_PROJECTION, String.class);
    Projection crs = Projection.of(rcrs);
    ProjectionImpl projection = null;
    if (crs instanceof ProjectionImpl p) {
      projection = p;
    } else {
      throw new KlabInternalErrorException(
          "WCS service expects a specific projection implementation, got " + rcrs + " instead");
    }

    double[] extent =
        space.getParameters().get(GeometryImpl.PARAMETER_SPACE_BOUNDINGBOX, double[].class);

    int xc = space.getShape().get(0).intValue();
    int yc = space.getShape().get(1).intValue();

    double west = extent[0];
    double east = extent[1];
    double south = extent[2];
    double north = extent[3];

    /*
     * jiggle by the projection's equivalent of a few meters if we're asking for a single point,
     * so WCS does not go crazy.
     */
    if (Utils.Numbers.equal(west, east)) {
      double delta =
          (projection
                      .getCoordinateReferenceSystem()
                      .getCoordinateSystem()
                      .getAxis(0)
                      .getMaximumValue()
                  - projection
                      .getCoordinateReferenceSystem()
                      .getCoordinateSystem()
                      .getAxis(0)
                      .getMinimumValue())
              / 3900000.0;
      west -= delta;
      east += delta;
    }

    if (Utils.Numbers.equal(north, south)) {
      double delta =
          (projection
                      .getCoordinateReferenceSystem()
                      .getCoordinateSystem()
                      .getAxis(1)
                      .getMaximumValue()
                  - projection
                      .getCoordinateReferenceSystem()
                      .getCoordinateSystem()
                      .getAxis(1)
                      .getMinimumValue())
              / 3900000.0;
      south -= delta;
      north += delta;
    }

    String s = null;

    if (version.getMajor() == 1) {
      if (version.getMinor() == 0) {

        s =
            serviceUrl
                + "?service=WCS&version="
                + version
                + "&request=GetCoverage&coverage="
                + layer.getRequestIdentifier()
                + "&bbox="
                + west
                + ","
                + south
                + ","
                + east
                + ","
                + north
                + "&crs="
                + projection.getCode()
                + "&responseCRS="
                + projection.getCode()
                + "&width="
                + xc
                + "&height="
                + yc
                + "&format="
                + "GeoTIFF";

      } else {

        // TODO WRONG!
        s =
            serviceUrl
                + "?service=WCS&version="
                + version
                + "&request=GetCoverage&identifier="
                + layer.getRequestIdentifier()
                + "&boundingbox="
                + west
                + ","
                + south
                + ","
                + east
                + ","
                + north
                + ","
                + projection.getCode()
                + "&responseCRS="
                + projection.getCode()
                + "&width="
                + xc
                + "&height="
                + yc
                + "&format="
                + "GeoTIFF";
      }
    } else if (version.getMajor() == 2) {
      // TODO
      // http://194.66.252.155/cgi-bin/BGS_EMODnet_bathymetry/ows?service=WCS&version=2.0.1&request=GetCoverage&CoverageId=BGS_EMODNET_AegeanLevantineSeas-MCol&format=image/png&subset=lat%2834.53627,38.88686%29&subset=long%2825.43366,31.32234%29&
      // http://194.66.252.155/cgi-bin/BGS_EMODnet_bathymetry/ows?service=WCS&version=2.0.1&request=GetCoverage&CoverageId=BGS_EMODNET_AegeanLevantineSeas-MCol&format=image/png&subset=lat,http://www.opengis.net/def/crs/EPSG/0/4326%2834.53627,38.88686%29&subset=long,http://www.opengis.net/def/crs/EPSG/0/4326%2825.43366,31.32234%29&
    } else {
      throw new KlabUnimplementedException("WCS version " + version + " is not supported");
    }

    /*
     * ACHTUNG this is a 2.0 only request
     */
    if (interpolation != null) {
      s += "&interpolation=" + interpolation.field;
    }

    try {
      url = new URL(s);
    } catch (MalformedURLException e) {
      throw new KlabInternalErrorException(e);
    }

    return url;
  }

  public boolean containsIdentifier(String string) {
    return identifiers.contains(string);
  }
}
