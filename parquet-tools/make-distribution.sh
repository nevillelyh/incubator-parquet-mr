#!/bin/bash

VERSION=$(mvn help:evaluate -Dexpression=project.version 2>/dev/null | grep "^[0-9][0-9A-Za-z.]\+$")
DISTDIR=parquet-tools-${VERSION}
mvn clean install
mvn dependency:copy-dependencies
mkdir -p ${DISTDIR}/bin/lib
mv target/dependency/*.jar ${DISTDIR}/bin/lib
cp target/*.jar ${DISTDIR}/bin/lib
cp src/main/scripts/* ${DISTDIR}/bin
