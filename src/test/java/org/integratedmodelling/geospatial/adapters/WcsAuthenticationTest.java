package org.integratedmodelling.geospatial.adapters;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import kong.unirest.GetRequest;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.integratedmodelling.geospatial.adapters.wcs.WCSServiceManager;
import org.integratedmodelling.klab.api.data.Version;
import org.integratedmodelling.klab.api.identities.UserIdentity;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.junit.jupiter.api.Test;

class WcsAuthenticationTest {
  @Test void publicServiceWithoutCallerReachesHttpWithoutOpeningTheCredentialVault() throws Exception {
    // Initialize Unirest before replacing its static request factory.
    Class.forName(WCSServiceManager.class.getName());
    var request = mock(GetRequest.class);
    @SuppressWarnings("unchecked") HttpResponse<String> response = mock(HttpResponse.class);
    when(request.asString()).thenReturn(response);
    when(response.getStatus()).thenReturn(503);
    when(response.getStatusText()).thenReturn("Fixture service unavailable");
    try (var http = mockStatic(Unirest.class)) {
      http.when(() -> Unirest.get(anyString())).thenReturn(request);
      var manager = assertDoesNotThrow(() -> new WCSServiceManager(
          "https://public.example.test/wcs", Version.create("2.0.1")));
      verify(request).asString();
      assertTrue(manager.hasErrors(), "The fixture HTTP error must be recorded, not an authentication failure");
    }
  }

  @Test void callerScopeIsForwardedAndServiceCacheIsIsolatedByUser() {
    WCSAdapter.services.clear();
    var first = caller("first");
    var second = caller("second");
    var arguments = new ArrayList<List<?>>();
    try (var managers = mockConstruction(WCSServiceManager.class,
        (manager, construction) -> arguments.add(new ArrayList<>(construction.arguments())))) {
      var version = Version.create("2.0.1");
      var a = WCSAdapter.getService("https://public.example.test/wcs", version, first);
      assertSame(a, WCSAdapter.getService("https://public.example.test/wcs", version, first));
      var b = WCSAdapter.getService("https://public.example.test/wcs", version, second);
      assertNotSame(a, b);
      assertEquals(2, managers.constructed().size());
      assertSame(first, arguments.get(0).get(2));
      assertSame(second, arguments.get(1).get(2));
    } finally {
      WCSAdapter.services.clear();
    }
  }

  private static ContextScope caller(String name) {
    var scope = mock(ContextScope.class);
    var user = mock(UserIdentity.class);
    when(user.isAuthenticated()).thenReturn(true);
    when(user.getUsername()).thenReturn(name);
    when(scope.getUser()).thenReturn(user);
    return scope;
  }
}

