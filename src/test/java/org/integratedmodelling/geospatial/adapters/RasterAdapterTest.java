//package org.integratedmodelling.geospatial.adapters;
//
//import org.integratedmodelling.geospatial.adapters.raster.BandMixing;
//import org.integratedmodelling.klab.api.data.Data;
//import org.integratedmodelling.klab.api.geometry.Geometry;
//import org.integratedmodelling.klab.api.knowledge.Observable;
//import org.integratedmodelling.klab.api.knowledge.Resource;
//import org.integratedmodelling.klab.api.knowledge.Urn;
//import org.integratedmodelling.klab.api.services.resources.impl.ResourceImpl;
//import org.integratedmodelling.klab.api.services.runtime.Notification;
//import org.integratedmodelling.klab.configuration.ServiceConfiguration;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.ValueSource;
//
//import java.io.File;
//import java.util.List;
//import java.util.NoSuchElementException;
//import java.util.stream.Collectors;
//
//class RasterAdapterTest {
//
//    @Test
//    @Disabled("WIP")
//    void importGeotiff() {
//    }
//
////    @Test
////    public void testEncodeFileNoParameters() {
////        boolean ignored = ServiceConfiguration.INSTANCE.allowAnonymousUsage();
////
////        var urn = Urn.of("klab:raster:test:colombia");
////        String centralColombia =
////                "τ0(1){ttype=LOGICAL,period=[1609459200000 1640995200000],"
////                        + "tscope=1.0,"
////                        + "tunit=YEAR}S2(934,631){bbox=[-75.2281407807369 -72.67107290964314 3.5641500380320963 5.302943221927137],"
////                        + "shape=00000000030000000100000005C0522AF2DBCA0987400C8361185B1480C052CE99DBCA0987400C8361185B1480C052CE99DBCA098740153636BF7AE340C0522AF2DBCA098740153636BF7AE340C0522AF2DBCA0987400C8361185B1480,proj=EPSG:4326}";
////
////        var observable = Observable.objects("porquerolles");
////        var geometry = Geometry.create(centralColombia);
//////        var builder = Data.builder("colombia", observable, geometry);
//////        var adapter = new RasterAdapter();
//////
//////        Resource resource = Resource.builder(urn.getUrn()).withGeometry(geometry).withAdapterType("raster").build();
//////
//////        File localFile = new File("src/test/resources/raster/utah_landcover.tif");
//////        ((ResourceImpl)resource).setLocalFiles(List.of(localFile));
//////
//////        adapter.encode(resource, Urn.of(urn.getUrn()), builder, geometry, observable, null);
//////
//////        var built = builder.build();
//////
//////        Assertions.assertFalse(built.empty());
////    }
//
////    @ParameterizedTest
////    @ValueSource(strings = {"max_value", "min_value", "avg_value", "sum_value", "band_max_value", "band_min_value"})
////    public void testEncoderSupportedBandmixing(String input) {
////        boolean ignored = ServiceConfiguration.INSTANCE.allowAnonymousUsage();
////
////        var urn = Urn.of("klab:raster:test:colombia#bandmixer=" + input);
////        String centralColombia =
////                "τ0(1){ttype=LOGICAL,period=[1609459200000 1640995200000],"
////                        + "tscope=1.0,"
////                        + "tunit=YEAR}S2(934,631){bbox=[-75.2281407807369 -72.67107290964314 3.5641500380320963 5.302943221927137],"
////                        + "shape=00000000030000000100000005C0522AF2DBCA0987400C8361185B1480C052CE99DBCA0987400C8361185B1480C052CE99DBCA098740153636BF7AE340C0522AF2DBCA098740153636BF7AE340C0522AF2DBCA0987400C8361185B1480,proj=EPSG:4326}";
////
////        var observable = Observable.objects("porquerolles");
////        var geometry = Geometry.create(centralColombia);
//////        var builder = Data.builder("colombia", observable, geometry);
//////        var adapter = new RasterAdapter();
//////
//////        Resource resource = Resource.builder(urn.getUrn()).withGeometry(geometry).withAdapterType("raster").build();
//////
//////        File localFile = new File("src/test/resources/raster/utah_landcover.tif");
//////        ((ResourceImpl)resource).setLocalFiles(List.of(localFile));
//////
//////        adapter.encode(resource, Urn.of(urn.getUrn()), builder, geometry, observable, null);
//////
//////        var built = builder.build();
//////        Assertions.assertFalse(built.empty());
////    }
//
//    @ParameterizedTest
//    @ValueSource(strings = {"okerra", "bandmixer"})
//    public void testEncoderFailUnsupportedBandmixing(String input) {
//        boolean ignored = ServiceConfiguration.INSTANCE.allowAnonymousUsage();
//
//        var urn = Urn.of("klab:raster:test:colombia#bandmixer=" + input);
//        String centralColombia =
//                "τ0(1){ttype=LOGICAL,period=[1609459200000 1640995200000],"
//                        + "tscope=1.0,"
//                        + "tunit=YEAR}S2(934,631){bbox=[-75.2281407807369 -72.67107290964314 3.5641500380320963 5.302943221927137],"
//                        + "shape=00000000030000000100000005C0522AF2DBCA0987400C8361185B1480C052CE99DBCA0987400C8361185B1480C052CE99DBCA098740153636BF7AE340C0522AF2DBCA098740153636BF7AE340C0522AF2DBCA0987400C8361185B1480,proj=EPSG:4326}";
//
//        var observable = Observable.objects("porquerolles");
//        var geometry = Geometry.create(centralColombia);
////        var builder = Data.builder("colombia", observable, geometry);
////        var adapter = new RasterAdapter();
////
////        Resource resource = Resource.builder(urn.getUrn()).withGeometry(geometry).withAdapterType("raster").build();
////
////        File localFile = new File("src/test/resources/raster/utah_landcover.tif");
////        ((ResourceImpl)resource).setLocalFiles(List.of(localFile));
////
////        Exception exception = Assertions.assertThrows(NoSuchElementException.class, () -> {
////            adapter.encode(resource, Urn.of(urn.getUrn()), builder, geometry, observable, null);
////        });
////
////        Assertions.assertEquals(NoSuchElementException.class, exception.getClass());
//    }
//
//}