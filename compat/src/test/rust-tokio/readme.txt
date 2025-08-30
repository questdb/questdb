What is Cargo.unlocked? It is renamed to Cargo.lock when we want
to test with known-to-work versions of dependencies. Then Cargo
uses Cargo.lock to resolve dependencies.

If we want to test with the latest versions of dependencies -
for compatibility purposes - then Cargo.unlocked is simple ignored
and version ranges from Cargo.toml are used as prescribed by semver.

When we decide to upgrade dependencies, we should ensure that
we test with both newer and older versions of dependencies. To catch
regressions with older client versions. This can be done by keeping
multiple Cargo.unlocked files around and CI run tests with each of them.