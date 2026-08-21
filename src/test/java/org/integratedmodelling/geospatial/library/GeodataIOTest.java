package org.integratedmodelling.geospatial.library;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Set;
import org.integratedmodelling.klab.api.data.KnowledgeGraph;
import org.integratedmodelling.klab.api.data.Metadata;
import org.integratedmodelling.klab.api.data.RuntimeAsset;
import org.integratedmodelling.klab.api.digitaltwin.DigitalTwin;
import org.integratedmodelling.klab.api.digitaltwin.GraphModel;
import org.integratedmodelling.klab.api.knowledge.Cohort;
import org.integratedmodelling.klab.api.knowledge.Concept;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.SemanticType;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;
import org.integratedmodelling.klab.api.knowledge.observation.scale.Scale;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Space;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.runtime.scale.space.ShapeImpl;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

class GeodataIOTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  @Test
  void exportsCollectiveObservationAndShapeBearingChildren() throws Exception {
    var root = observation(1, "context:collective", "Animals", true, point(1, 2));
    var child = observation(2, "context:animal:2", "Animal 2", false, point(3, 4));
    var childWithoutShape = observation(3, "context:animal:3", "Animal 3", false, null);
    var scope = mock(ContextScope.class);
    when(scope.getChildrenOf(root)).thenReturn(List.of(child, childWithoutShape));

    JsonNode json =
        JSON.readTree(new GeodataIO().exportGeoJSON(root, scope).readAllBytes());

    assertEquals("FeatureCollection", json.get("type").asText());
    assertEquals(2, json.get("features").size());
    assertEquals("asset", json.at("/features/0/properties/relationship").asText());
    assertEquals("child", json.at("/features/1/properties/relationship").asText());
    assertEquals("Animals", json.at("/features/0/properties/name").asText());
    assertEquals(3, json.at("/features/1/geometry/coordinates/0").asInt());
    assertEquals(4, json.at("/features/1/geometry/coordinates/1").asInt());
  }

  @Test
  void exportsCohortAndMembersThroughHasMemberLinks() throws Exception {
    var cohort = mock(Cohort.class);
    var observable = observable("test:animal", true);
    var cohortScale = scale(point(-2, 40));
    when(cohort.getId()).thenReturn(10L);
    when(cohort.classify()).thenReturn(RuntimeAsset.Type.COHORT);
    when(cohort.getUrn()).thenReturn("context:cohort:test:animal");
    when(cohort.getObservable()).thenReturn(observable);
    when(cohort.getGeometry()).thenReturn(cohortScale);
    when(cohort.getMetadata()).thenReturn(Metadata.create("source", "cohort"));

    var member = observation(11, "context:animal:11", "Animal 11", false, point(-1, 41));
    var link = mock(KnowledgeGraph.Link.class);
    when(link.target()).thenReturn(member);

    var graph = mock(KnowledgeGraph.class);
    var digitalTwin = mock(DigitalTwin.class);
    var scope = mock(ContextScope.class);
    when(scope.getDigitalTwin()).thenReturn(digitalTwin);
    when(digitalTwin.getKnowledgeGraph()).thenReturn(graph);
    when(graph.getLinks(
            cohort,
            GraphModel.Relationship.Direction.OUTGOING,
            scope,
            GraphModel.Relationship.HAS_MEMBER))
        .thenReturn(List.of(link));

    JsonNode json =
        JSON.readTree(new GeodataIO().exportGeoJSON(cohort, scope).readAllBytes());

    assertEquals(2, json.get("features").size());
    assertEquals("COHORT", json.at("/features/0/properties/assetType").asText());
    assertEquals("member", json.at("/features/1/properties/relationship").asText());
    assertEquals("cohort", json.at("/features/0/properties/metadata/source").asText());
    assertTrue(json.at("/features/0/properties/collective").asBoolean());
  }

  private static Observation observation(
      long id, String urn, String name, boolean collective, Geometry geometry) {
    var observation = mock(Observation.class);
    var observable = observable("test:animal", collective);
    var observationScale = scale(geometry);
    when(observation.getId()).thenReturn(id);
    when(observation.classify()).thenReturn(RuntimeAsset.Type.OBSERVATION);
    when(observation.getUrn()).thenReturn(urn);
    when(observation.getName()).thenReturn(name);
    when(observation.getObservable()).thenReturn(observable);
    when(observation.getGeometry()).thenReturn(observationScale);
    when(observation.getMetadata()).thenReturn(Metadata.create());
    return observation;
  }

  private static Observable observable(String urn, boolean collective) {
    var concept = mock(Concept.class);
    when(concept.getType()).thenReturn(Set.of(SemanticType.SUBJECT));
    when(concept.isCollective()).thenReturn(collective);
    var observable = mock(Observable.class);
    when(observable.getUrn()).thenReturn(urn);
    when(observable.getSemantics()).thenReturn(concept);
    return observable;
  }

  private static Scale scale(Geometry geometry) {
    var scale = mock(Scale.class);
    if (geometry == null) {
      return scale;
    }
    var space = mock(Space.class);
    var shape = mock(ShapeImpl.class);
    when(shape.isEmpty()).thenReturn(false);
    when(shape.getStandardizedGeometry()).thenReturn(geometry);
    when(space.getGeometricShape()).thenReturn(shape);
    when(scale.getSpace()).thenReturn(space);
    return scale;
  }

  private static Geometry point(double x, double y) {
    return GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
  }
}
