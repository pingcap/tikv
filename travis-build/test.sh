#!/usr/bin/env bash
# Copyright 2016 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -o pipefail

panic() {
    echo -e "$@" >&1
    exit 1
}

make format
git diff-index --quiet HEAD -- || panic "\e[35mplease make format before creating a pr!!!\e[0m" 

trap 'kill $(jobs -p) &> /dev/null || true' EXIT

if [[ "$ENABLE_FEATURES" = "" ]]; then
    export ENABLE_FEATURES=dev
fi
export LOG_FILE=tests.log
export RUST_TEST_THREADS=1
export RUSTFLAGS=-Dwarnings

NO_RUN="--no-run" make test
status=$?

if [[ "$SKIP_TESTS" = "" ]]; then
    # start pd
    which pd-server
    if [ $? -eq 0 ] && [[ "$TRAVIS" != "true" ]]; then
        # Separate PD clusters.
        pd-server --name="pd1" \
            --data-dir="default.pd1" \
            --client-urls="http://:12379" \
            --peer-urls="http://:12380" &
        pd-server --name="pd2" \
            --data-dir="default.pd2" \
            --client-urls="http://:22379" \
            --peer-urls="http://:22380" &
        sleep 3s
        export PD_ENDPOINTS=127.0.0.1:12379
        export PD_ENDPOINTS_SEP=127.0.0.1:22379
    fi
    make test 2>&1 | tee tests.out
else
    exit 0
fi
status=$?
for case in `cat tests.out | python -c "import sys
import re
p = re.compile(\"thread '([^']+)' panicked at\")
cases = set()
for l in sys.stdin:
    l = l.strip()
    m = p.search(l)
    if m:
        cases.add(m.group(1).split(':')[-1])
print '\n'.join(cases)
"`; do
    echo find fail cases: $case
    grep $case $LOG_FILE | cut -d ' ' -f 2-
    # there is a thread panic, which should not happen.
    status=1
    echo
done

# don't remove the tests.out, coverage counts on it.
rm $LOG_FILE || true

exit $status
