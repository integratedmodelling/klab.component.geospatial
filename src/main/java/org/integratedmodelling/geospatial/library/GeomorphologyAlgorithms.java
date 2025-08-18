package org.integratedmodelling.geospatial.library;

import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.data.Storage;
import org.integratedmodelling.klab.api.data.mediation.NumericRange;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.geometry.Geometry.Dimension;
import org.integratedmodelling.klab.api.knowledge.Artifact.Type;
import org.integratedmodelling.klab.api.knowledge.observation.scale.Scale;
import org.integratedmodelling.klab.api.lang.ServiceCall;
import org.integratedmodelling.klab.api.services.runtime.extension.KlabFunction;
import org.integratedmodelling.klab.api.services.runtime.extension.Library;

// TODO provide implementations for the entire HortonMachine :)
@Library(
    name = "klab.geomorphology",
    description =
        """
        Contextualizers that ....""")
public class GeomorphologyAlgorithms {

  // FIXME this only as an example, copied from klab.component.generators
  @KlabFunction(
      name = "slope",
      description =
          """
            Generate fractal surfaces within a user-defined range \
            and with a configurable degree of smoothness, apt to simulating several terrain patterns such as \
            elevation or slope. As the generator works in RAM, this should not be used on very large grids.""",
      geometry = "S2",
      type = Type.NUMBER,
      split = 1, // FIXME this should be unnecessary because the single DoubleBuffer arg should force it
      fillingCurve = Data.FillCurve.D2_XY,
      parameters = {
        @KlabFunction.Argument(
            name = "range",
            type = Type.RANGE,
            description = "The min-max range of the values produced. Default is 0 to 4000",
            optional = true),
        @KlabFunction.Argument(
            name = "detail",
            type = Type.NUMBER,
            description =
                "Controls the amount of detail in the generated structure. Default is 8, "
                    + "appropriate for geographical elevation",
            optional = true),
        @KlabFunction.Argument(
            name = "roughness",
            type = Type.NUMBER,
            description =
                "Controls the roughness of the generated terrain. Default is 0.55, "
                    + "appropriate"
                    + " for geographical elevation",
            optional = true)
      })
  public void generateTerrain(Storage.DoubleBuffer storage, Scale scale, ServiceCall call) {

    var range = call.getParameters().get("range", NumericRange.create(0., 4000., false, false));
    var xy = scale.getSpace().getShape();
    var xx = xy.get(0);
    var yy = xy.get(1);
//    var terrain =
//        new Terrain(
//            call.getParameters().get("detail", 8),
//            call.getParameters().get("roughness", 0.55),
//            range.getLowerBound(),
//            range.getUpperBound());

    var filler = storage.scan();
    /*
     * appropriate pattern for generic scale when we handle only one dimension, even if in most
     * situations (all at the moment) only one subscale will be returned. If there is no time or
     * other dimension, a unit scale will be returned and at(unit) will later return self. Inside
     * the loop, adapt the overall geometry located by each sub-scale to a grid scanner and use a
     * buffer for fast access to storage. The geometry requirement ensures that we get a regular 2D
     * spatial extent, so this is safe w/o error checking.
     */
    for (Geometry subscale : scale.without(Dimension.Type.SPACE)) {

      // this scale has every dimension localized except space
      var spaceScale = scale.at(subscale);
      // choose the fill curve that best represents the problem
      double dx = 1.0 / (double) xx;
      double dy = 1.0 / (double) yy;

      for (int x = 0; x < xx; x++) {
        for (int y = 0; y < yy; y++) {
//          filler.add(terrain.getAltitude(x * dx, y * dy));
        }
      }
    }
  }
}
