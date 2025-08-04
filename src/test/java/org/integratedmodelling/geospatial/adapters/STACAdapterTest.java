package org.integratedmodelling.geospatial.adapters;

import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.exceptions.KlabUnimplementedException;
import org.integratedmodelling.klab.api.knowledge.Artifact;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class STACAdapterTest {

    @Test
    public void validateGetSupportedType() {
        Parameters params = Parameters.create("collection", "https://planetarycomputer.microsoft.com/api/stac/v1/collections/io-lulc-annual-v02", "asset", "data");
        Resource res = Resource.builder("klab:raster:test:colombia").withParameters(params).build();

        var type = new STACAdapter().getType(res);

        Assertions.assertEquals(Artifact.Type.NUMBER, type);
    }

    @Test
    public void validateFailUnsupportedType() {
        Parameters params = Parameters.create("collection", "https://planetarycomputer.microsoft.com/api/stac/v1/collections/io-lulc-annual-v02", "asset", "rendered_preview");
        Resource res = Resource.builder("klab:raster:test:colombia").withParameters(params).build();

        Exception exception = Assertions.assertThrows(KlabUnimplementedException.class, () -> {
            new STACAdapter().getType(res);
        });

        Assertions.assertEquals(KlabUnimplementedException.class, exception.getClass());

    }


}