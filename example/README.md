
```
sbt stage
sbt example/universal:packageZipTarbal
stage/bin/entrypoint -Dtasks.cache.enabled=false -Dhosts.numCPU=0 -Dtasks.elastic.engine=SH -Dtasks.elastic.bin=example-0.1-SNAPSHOT.tgz
```
