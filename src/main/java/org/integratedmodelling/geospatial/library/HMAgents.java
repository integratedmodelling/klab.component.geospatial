package org.integratedmodelling.geospatial.library;

import org.integratedmodelling.common.logging.Logging;
import org.integratedmodelling.klab.api.actors.RuntimeAgent;
import org.integratedmodelling.klab.api.services.runtime.extension.Actor;
import org.integratedmodelling.klab.api.services.runtime.extension.Library;
import org.integratedmodelling.klab.api.services.runtime.extension.Verb;

// TODO provide implementations for the entire HortonMachine :)
@Library(
    name = "hm",
    description =
        """
                Agents implementing HortonMachine processors""")
public class HMAgents {

    @Actor(name = "raster", description = "Raster processor")
    public static class Raster {

        // TODO needs an adapter of an observation to a Raster and back

        @Verb(name = "flowdirections", description = "Just testing", executionType = Verb.Type.FUNCTION)
        public static void info(RuntimeAgent.Scope scope, Object... messages) {
            var uscope = scope.getScope();
            if (uscope != null) {
                uscope.info(messages);
            } else {
                Logging.INSTANCE.info(messages);
            }
        }
    }


}
