#!/bin/sh
# Run the real YCSB 0.17.0 load phase (one client thread) and write every
# record as TSV, the format of `oltp-gen ycsb --format text`; or, with
# YCSB_MODE=-t and `-p refdb.run=true`, the run phase, one line per binding
# call, the format of `oltp-gen ycsb-run --format text`.
#
#   ycsb-ref/run.sh OUT.tsv [-p key=value ...]
#   YCSB_MODE=-t ycsb-ref/run.sh OUT.tsv -p refdb.run=true -p operationcount=N ...
#
# `-p refdb.seed=S` sets the client thread's ThreadLocalRandom seed before
# the first operation, so random values are reproducible; the thread id it
# prints goes to `oltp-gen ycsb --java-thread`. Needs a JDK (17+) and curl;
# YCSB's core sources and its dependencies are fetched from Maven Central
# into $YCSB_REF_DIR (default: /tmp/ycsb-ref) once.
set -e
OUT=$1; shift
DIR=${YCSB_REF_DIR:-/tmp/ycsb-ref}
HERE=$(cd "$(dirname "$0")" && pwd)
M=https://repo1.maven.org/maven2
mkdir -p "$DIR"
if [ ! -f "$DIR/classes/RefDB.class" ]; then
  cd "$DIR"
  curl -sfO --retry 6 --retry-delay 10 $M/site/ycsb/core/0.17.0/core-0.17.0-sources.jar
  curl -sfO --retry 6 --retry-delay 10 $M/org/apache/htrace/htrace-core4/4.1.0-incubating/htrace-core4-4.1.0-incubating.jar
  curl -sfO --retry 6 --retry-delay 10 $M/org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar
  curl -sfO --retry 6 --retry-delay 10 $M/org/codehaus/jackson/jackson-core-asl/1.9.4/jackson-core-asl-1.9.4.jar
  curl -sfO --retry 6 --retry-delay 10 $M/org/codehaus/jackson/jackson-mapper-asl/1.9.4/jackson-mapper-asl-1.9.4.jar
  mkdir -p src classes && (cd src && unzip -qo ../core-0.17.0-sources.jar)
  CP=$(ls "$DIR"/*.jar | tr '\n' :)
  javac -nowarn -d classes -cp "$CP" $(find src -name '*.java') "$HERE/RefDB.java"
  cd - >/dev/null
fi
CP="$DIR/classes:$(ls "$DIR"/*.jar | tr '\n' :)"
exec java --add-opens java.base/java.lang=ALL-UNNAMED -cp "$CP" site.ycsb.Client "${YCSB_MODE:--load}" \
  -db RefDB -threads 1 -p workload=site.ycsb.workloads.CoreWorkload -p refdb.out="$OUT" "$@"
