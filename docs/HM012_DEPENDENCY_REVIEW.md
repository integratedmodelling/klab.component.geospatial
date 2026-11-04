# HortonMachine 0.12.0 / GeoTools 34.4 proposal

Author-agent review using `KLAB_CLI_Contribution_Review.md`; not maintainer approval.
Base: upstream `main` at `3f98082db59e4b0b82eecff0e5753eec41fed671`.
No content from the separate, closed exporter PR #9 is included.

## Change and independently reproduced defect

Replace the moving HM 0.11.3-SNAPSHOT with release 0.12.0. Keep GeoTools 34.4,
JTS 1.20.0, Jackson 2.19.0 and H2GIS 1.5.0. Explicitly manage H2 1.4.197,
the version required by H2GIS 1.5.0. The imported platform otherwise selects
H2 1.1.119; actually initializing H2GIS then fails with
`NoClassDefFoundError: org/h2/api/Aggregate`. The same in-memory probe with
H2 1.4.197 passes; a regression now checks actual extension loading and the
independent rectangle-area oracle 4 * 3 = 12 coordinate-units squared.
No geographic-area or published-study replication is claimed.

## Validation and boundaries

JDK Temurin 21.0.12.1, Maven 3.9.5: `mvn -B -nsu test package` passes all
9 tests, no failures/errors/skips, on this upstream-main-based proposal.
The separate preserved exporter branch also passed 22 tests with an explicit
HM0.12 override; those are not tests of this proposal's nine-test source tree.
The old managed-H2 failure and successful isolated substitution remain in the
consolidation evidence; no databases, services or shared systems were touched.

**Not merge-ready:** the component packager reports host-provided GeoTools34.4
versus raw transitive34.6 and Jackson2.19 versus raw transitive2.21 exclusions.
Unit-classpath success does not establish the packaged component's actual host
classloader behavior, particularly H2. Verify the final host H2 resolution and
component loading, resolved hashes and startup before merging or publishing.
HM0.12's own release baseline is GT34.6; forcing34.4 needs that final check.
No GeoTools35, deployment, Maven publication, snapshot upgrade or legacy database
migration is included. Existing baseline remains the deployment baseline.

Recommendation: **draft focused proposal pending host/plugin acceptance**, not an
adopted dependency release. Upstream write permission is unavailable; submit from
the existing contributor fork through the normal review process.
