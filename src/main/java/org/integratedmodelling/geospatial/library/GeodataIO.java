package org.integratedmodelling.geospatial.library;

import org.apache.tika.mime.MediaType;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.digitaltwin.Scheduler;
import org.integratedmodelling.klab.api.knowledge.KlabAsset;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.services.resources.adapters.Exporter;
import org.integratedmodelling.klab.api.services.runtime.extension.Library;
import org.integratedmodelling.klab.services.base.BaseService;

import java.io.InputStream;

@Library(
    name = "geospatial.io",
    description =
"""
GeoTIFF export for gridded spatial data.
""")
public class GeodataIO {
  @Exporter(
      schema = "geotiff",
      geometry = "S2",
      knowledgeClass = KlabAsset.KnowledgeClass.OBSERVATION,
      mediaType = "image/tiff",
      description = "Export an observation as HTML page visualizing it")
  public InputStream exportHtml(
      Resource resource,
      Observation observation,
      Scheduler.Event event,
      ContextScope scope,
      BaseService service,
      Parameters<String> parameters) {
    return null;
  }
}
