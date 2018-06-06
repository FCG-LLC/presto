#!/usr/bin/env bash

# override launch settings
cp -r /opt/presto/config/. /opt/presto/etc/

# run presto
/opt/presto/bin/launcher run