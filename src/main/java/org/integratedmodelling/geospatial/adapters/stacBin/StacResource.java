package org.integratedmodelling.geospatial.adapters.stacBin;

import java.io.File;
import java.io.IOException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.hortonmachine.gears.utils.geometry.GeometryUtilities;
import org.integratedmodelling.klab.api.data.Data;
import org.integratedmodelling.klab.api.knowledge.observation.scale.space.Space;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.Time;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.runtime.scale.space.EnvelopeImpl;
import org.hortonmachine.gears.io.rasterreader.OmsRasterReader;

public class StacResource{

    public GridCoverage2D getCoverage(Data.Builder builder, Space space, Time time, String assetId, Scope scope, String url) throws Exception {
        try {
            var envelope = space.getEnvelope();
            double[] bbox = {envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()};
            var start = time.getStart();
            var end = time.getEnd();
            String binaryPath = "./myscript"; // or "myscript.exe" on Windows
            File tempFile = File.createTempFile("geo", ".tif");
            tempFile.deleteOnExit();

            ProcessBuilder pb = new ProcessBuilder(
                binaryPath,
                "--bbox", String.valueOf(bbox[0]), String.valueOf(bbox[1]), String.valueOf(bbox[2]), String.valueOf(bbox[3]),
                "--time", start.toString(), end.toString(),
                "--asset", assetId,
                "--collection", url,
                "--output", tempFile.getAbsolutePath()
            );

            pb.inheritIO(); // Optional: forwards stdout/stderr to console

            Process process = pb.start();
            int exitCode = process.waitFor(); // Wait for the binary to finish
            if (exitCode != 0) {
                throw new RuntimeException("Binary execution failed with exit code: " + exitCode);
            }

            GridCoverage2D coverage = OmsRasterReader.readRaster(tempFile.getAbsolutePath());			
            return coverage;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;   
    }
}
