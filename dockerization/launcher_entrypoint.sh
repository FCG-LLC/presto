#!/usr/bin/env bash

if [[ "$1" == "version" ]]; then
  exec /usr/bin/version
fi

# override launch settings
cp -r /opt/presto/config/. /opt/presto/etc/

# run presto
exec /opt/presto/bin/launcher run
