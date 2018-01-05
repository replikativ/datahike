#!/bin/sh

set -e

(cat release-js/wrapper.prefix; cat release-js/datahike.bare.js; cat release-js/wrapper.suffix) > release-js/datahike.js

echo "Packed release-js/datahike.js"
