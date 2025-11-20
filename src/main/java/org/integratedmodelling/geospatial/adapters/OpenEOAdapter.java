package org.integratedmodelling.geospatial.adapters;

import kong.unirest.json.JSONObject;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.knowledge.Artifact;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.Urn;
import org.integratedmodelling.klab.api.knowledge.observation.scale.Scale;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.services.resources.adapters.Parameter;
import org.integratedmodelling.klab.api.services.resources.adapters.ResourceAdapter;

@ResourceAdapter(
        name = "openeo",
        version = Version.CURRENT,
        embeddable = true,
        parameters = {
                @Parameter(
                        name = "serviceUrl",
                        type = Artifact.Type.URL,
                        description =
                                "The URL of the service providing the data."),
                @Parameter(
                        name = "processId",
                        type = Artifact.Type.TEXT,
                        description =
                                "If the resource only calls one process at server side with arguments, name the process here."),
                @Parameter(
                        name = "namespace",
                        type = Artifact.Type.URL,
                        description =
                                "Public URL of a process definition that will be added to the namespace during contextualization."),
                @Parameter(
                        name = "nodata",
                        type = Artifact.Type.NUMBER,
                        description =
                                "The no-data value for this raster."),
        })
public class OpenEOAdapter {

    public OpenEOAdapter() {
    }

    @ResourceAdapter.Encoder
    public void encode(Resource resource, Urn urn, Data.Builder builder, Geometry geometry, Observable observable, Scope scope) {
        boolean synchronous = resource.getParameters().containsKey("synchronous")
                ? Boolean.parseBoolean(resource.getParameters().get("synchronous", String.class))
                : false;

        OpenEO service = OpenEOAdapter.getClient(resource.getParameters().get("serviceUrl").toString());
        if (service != null && service.isOnline()) {

            JSONObject arguments = new JSONObject();
            Scale rscal = Scale.create(resource.getGeometry());
            Scale scale = geometry instanceof Scale ? (Scale) geometry : Scale.create(geometry);

            /*
             * check for urn parameters that are recognized and match the type of the inputs
             */
            for (String parameter : resource.getParameters().keySet()) {
                if (!knownParameters.contains(parameter)) {
                    arguments.put(parameter, Utils.asPOD(urnParameters.get(parameter)));
                }
            }

            // resource has space: must specify space
            if (rscal.getSpace().isRegular() && scale.getSpace().size() > 1) {

                IGrid grid = ((Space) scale.getSpace()).getGrid();

                if (grid == null) {
                    throw new KlabIllegalStateException("running a gridded OpenEO process in a non-grid context");
                }

                /*
                 * must have space.resolution and space.shape parameters
                 */
                if (resource.getParameters().containsKey("space.shape")
                        && resource.getParameters().containsKey("space.resolution")) {

                    /*
                     * set GeoJSON shape and x,y resolution in parameters
                     */
                    arguments.put(resource.getParameters().get("space.shape", String.class),
                            ((Shape) scale.getSpace().getShape()).asGeoJSON());

                    List<Number> resolution = new ArrayList<>();
                    resolution.add(grid.getCellWidth());
                    resolution.add(grid.getCellHeight());

                    arguments.put(resource.getParameters().get("space.resolution", String.class),
                            grid.getCellWidth()/* resolution */);

                } else {
                    throw new KlabIllegalStateException(
                            "resource does not specify enough space parameters to contextualize");
                }

            } else if (rscal.getSpace() != null) {
                /*
                 * must have space.shape, set that and see what happens
                 */
                if (resource.getParameters().containsKey("space.shape")) {
                    arguments.put(resource.getParameters().get("space.shape", String.class),
                            ((Shape) scale.getSpace().getShape()).asGeoJSON());
                } else {
                    throw new KlabIllegalStateException(
                            "resource does not specify enough space parameters to contextualize");
                }
            }

            if (scale.getSpace() != null && resource.getParameters().contains("space.projection")) {
                Object projectionData = scale.getSpace().getProjection().getSimpleSRS().startsWith("EPSG:")
                        ? Integer.parseInt(scale.getSpace().getProjection().getSimpleSRS().substring(5))
                        : ((Projection) scale.getSpace().getProjection()).getWKTDefinition();
                arguments.put(resource.getParameters().get("space.projection", String.class), projectionData);
            }

            // resource is temporal: must specify extent
            if (rscal.getTime() != null) {
                /*
                 * must have either time.year or time.extent parameter
                 */
                if (resource.getParameters().containsKey("time.year")) {
                    if (scale.getTime().getResolution().getType() == Type.YEAR
                            && scale.getTime().getResolution().getMultiplier() == 1) {
                        arguments.put(resource.getParameters().get("time.year", String.class),
                                scale.getTime().getStart().getYear());
                    } else {
                        throw new KlabUnsupportedFeatureException("non-yearly use of yearly OpenEO resource");
                    }
                } else if (resource.getParameters().containsKey("time.extent")) {

                    List<String> range = new ArrayList<>();
                    range.add(scale.getTime().getStart().toRFC3339String());
                    range.add(scale.getTime().getEnd().toRFC3339String());
                    arguments.put(resource.getParameters().get("time.extent", String.class), range);

                } else {
                    throw new KlabIllegalStateException(
                            "resource does not specify enough temporal parameters to contextualize");
                }
            }

            List<Process> processes = new ArrayList<>();
            File processDefinition = new File(resource.getLocalPath() + File.separator + "process.json");
            if (processDefinition.isFile()) {
                processes.add(JsonUtils.load(processDefinition, Process.class));
            } else if (resource.getParameters().containsKey("namespace")) {
                try {
                    Process process = JsonUtils.load(new URL(resource.getParameters().get("namespace", String.class)),
                            Process.class);
                    process.encodeSelf(resource.getParameters().get("namespace", String.class));
                    processes.add(process);
                } catch (KlabIOException | MalformedURLException e) {
                    // dio stracane
                    throw new KlabIOException(e);
                }
            }

            if (synchronous) {

                RasterEncoder encoder = new RasterEncoder();
                try {
                    service.runJob(resource.getParameters().get("processId", String.class), arguments,
                            scope.getMonitor(), (input) -> {
                                File outfile = WcsEncoder.getAdjustedCoverage(input, geometry);
                                encoder.encodeFromCoverage(resource, urnParameters, encoder.readCoverage(outfile),
                                        geometry, builder, scope);
                            }, processes.toArray(new Process[processes.size()]));

                } catch (Throwable t) {
                    scope.getMonitor().error(t);
                    throw t;
                }
            } else {

                OpenEOFuture job = service.submit(resource.getParameters().get("processId", String.class), arguments,
                        scope.getMonitor(), processes.toArray(new Process[processes.size()]));

                try {
                    Map<String, Object> results = job.get();

                    if (job.isCancelled()) {
                        scope.getMonitor().warn("job canceled");
                    } else if (job.getError() != null) {
                        scope.getMonitor().error(job.getError());
                    } else {

                        for (String key : results.keySet()) {
                            Map<?, ?> result = (Map<?, ?>) results.get(key);
                            if (result.containsKey("href") && result.containsKey("type")) {
                                /*
                                 * depending on the geometry, this may be of different types
                                 */
                                if (result.get("type").toString().contains("geotiff")) {
                                    File outfile = WcsEncoder.getAdjustedCoverage(result.get("href").toString(),
                                            geometry);
                                    RasterEncoder encoder = new RasterEncoder();
                                    encoder.encodeFromCoverage(resource, urnParameters, encoder.readCoverage(outfile),
                                            geometry, builder, scope);
                                    break;
                                }
                                // TODO handle other cases
                            }
                        }

                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new KlabInternalErrorException(e);
                }
            }

        } else {
            scope.getMonitor().warn("resource " + resource.getUrn() + " went offline");
        }    }

    @ResourceAdapter.Type
    public Artifact.Type getType(Resource resourceUrn) {
        return Artifact.Type.NUMBER;
    }

    @ResourceAdapter.Type
    public static Artifact.Type getType(String collection, String assetId) {
        return Artifact.Type.NUMBER;
    }

    @ResourceAdapter.Validator(phase = ResourceAdapter.Validator.LifecyclePhase.LocalImport)
    public boolean validateLocalImport(String collectionUrl) {
        //TODO
        return true;
    }
}
