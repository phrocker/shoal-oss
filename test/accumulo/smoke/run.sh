#!/usr/bin/env bash
set -euo pipefail

mode="${1:?expected ready or smoke}"
classes=/tmp/shoal-accumulo4-smoke-classes
classpath="$(accumulo classpath)"

rm -rf "${classes}"
mkdir -p "${classes}"
javac -cp "${classpath}" -d "${classes}" /opt/shoal-smoke/AccumuloSmoke.java
exec java -cp "${classes}:${classpath}" AccumuloSmoke "${mode}"
