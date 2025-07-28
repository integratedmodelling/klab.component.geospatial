package org.integratedmodelling.geospatia.adapters;

import org.integratedmodelling.geospatial.adapters.RasterAdapter;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.Resource;
import org.integratedmodelling.klab.api.knowledge.Urn;
import org.integratedmodelling.klab.api.services.resources.impl.ResourceImpl;
import org.integratedmodelling.klab.configuration.ServiceConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

public class RasterAdapterTest {

    @BeforeAll
    public void beforeAll() {
        // This sets up the k.LAB environment for the service side
        var ignored = ServiceConfiguration.INSTANCE.allowAnonymousUsage();
    }

    @Test
    public void testEncode() {
        Urn urn = Urn.of("klab:raster:test:colombia");
        String centralColombia =
                "τ0(1){ttype=LOGICAL,period=[1609459200000 1640995200000],tscope=1.0,"
                        + "tunit=YEAR}S2(934,631){bbox=[-75.2281407807369 -72.67107290964314 3.5641500380320963 5"
                        + ".302943221927137],"
                        + "shape"
                        + "=00000000030000000100000005C0522AF2DBCA0987400C8361185B1480C052CE99DBCA0987400C8361185B1480C052CE99DBCA098740153636BF7AE340C0522AF2DBCA098740153636BF7AE340C0522AF2DBCA0987400C8361185B1480,proj=EPSG:4326}";

        var observable = Observable.objects("porquerolles");
        var geometry = Geometry.create(centralColombia);
        var builder = Data.builder("colombia", observable, geometry);
        var adapter = new RasterAdapter();
        Resource resource = Resource.builder(urn.getUrn()).withGeometry(geometry).withAdapterType("raster").build();
        File localFile = new File("./test.tiff");
        ((ResourceImpl)resource).setLocalFiles(List.of(localFile));
        adapter.encode(resource, Urn.of(urn.getUrn()), builder, geometry, observable, null);

        var built = builder.build();
        System.out.println(built);
    }

}
