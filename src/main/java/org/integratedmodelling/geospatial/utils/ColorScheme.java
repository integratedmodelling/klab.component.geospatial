package org.integratedmodelling.geospatial.utils;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.integratedmodelling.klab.api.collections.Triple;
import org.integratedmodelling.klab.api.data.mediation.Unit;
import org.integratedmodelling.klab.api.digitaltwin.Scheduler;
import org.integratedmodelling.klab.api.knowledge.Artifact;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;

/**
 * Complex serializable color schema that pairs numeric values or concept values to colors,
 * optionally with an expression that selects a specific schema among many, and with units to ensure
 * that the colormap defined matches data of any semantics. These can be saved in JSON and loaded.
 *
 * @author ferdinando.villa
 */
public class ColorScheme {

  private String name;
  private String valueUnit;
  private String expression;
  private List<Map<String, List<Integer>>> numberColors = new ArrayList<>();
  private List<Map<String, List<Integer>>> categoryColors = new ArrayList<>();
  private boolean relative;

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public boolean isRelative() {
    return relative;
  }

  public void setRelative(boolean relative) {
    this.relative = relative;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValueUnit() {
    return valueUnit;
  }

  public void setValueUnit(String valueUnit) {
    this.valueUnit = valueUnit;
  }

  public List<Map<String, List<Integer>>> getNumberColors() {
    return numberColors;
  }

  public void setNumberColors(List<Map<String, List<Integer>>> numberColors) {
    this.numberColors = numberColors;
  }

  public List<Map<String, List<Integer>>> getCategoryColors() {
    return categoryColors;
  }

  public void setCategoryColors(List<Map<String, List<Integer>>> categoryColors) {
    this.categoryColors = categoryColors;
  }

  /**
   * Compute values, colors and labels for the passed state.
   *
   * @param state
   * @return
   */
  public Triple<double[], Color[], String[]> computeScheme(
      Observation state, Scheduler.Event locator) {

    Triple<double[], Color[], String[]> ret = null;

    Unit unitFrom = null;
    Unit unitTo = null;
    if (this.valueUnit != null) {
//      unitFrom = UnitService.create(this.valueUnit);
      unitTo = state.getObservable().getUnit();
      if (unitTo == null) {
        return null;
      }
    }

    int mapIndex = 0;
    if (expression != null) {
      // TODO - compute the mapIndex passing the state and the locator
    }

    if (getNumberColors().size() > 0) {
      if (state.getType() != Artifact.Type.NUMBER) {
        return null;
      }

      Map<String, List<Integer>> map = getNumberColors().get(mapIndex);

      /*
       * This counts on the fact that Jackson uses a LinkedMapSet, so the values are
       * ordered.
       */
      double[] values = new double[map.size()];
      Color[] colors = new Color[map.size()];
      String[] labels = new String[map.size()];
      int i = 0;

////      StateSummary summary =
////          relative ? Observations.INSTANCE.getStateSummary(state, locator) : null;
////
////      for (String key : map.keySet()) {
////        List<Integer> rgb = map.get(key);
////        double value = Double.parseDouble(key);
////
////        if (relative) {
////          value =
////              summary.getRange().get(0)
////                  + (value * (summary.getRange().get(1) - summary.getRange().get(0)));
////        } else if (unitFrom != null && !unitFrom.equals(unitTo)) {
////          value = unitTo.convert(value, unitFrom).doubleValue();
////        }
//
//        values[i] = value;
//        colors[i] = new Color(rgb.get(0), rgb.get(1), rgb.get(2));
//        labels[i] = key;
//        i++;
//      }
//
//      ret = Triple.of(values, colors, labels);

    } else if (getCategoryColors().size() > 0) {
      if (state.getType() != Artifact.Type.CONCEPT) {
        return null;
      }
    }

    return ret;
  }
}
