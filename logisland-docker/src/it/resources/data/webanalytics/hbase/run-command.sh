#!/usr/bin/env bash

# A script that executes one or more command(s) provided as argument to hbase in non-interactive mode.

cmd="${@}"
/usr/local/hbase/hbase-1.1.2/bin/hbase shell <<EOF
${cmd}
quit
EOF
