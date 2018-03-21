#!/bin/sh
cd ${AZ_BATCH_APP_PACKAGE_ukwac}
java -Xmx30G -cp ukwebarchive-0.1-SNAPSHOT-jar-with-dependencies.jar $1 $2
