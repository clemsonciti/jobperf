#!/bin/bash

set -e

BUILD_DIR=$(mktemp -d)
echo "building in $BUILD_DIR..."
curl -Lfo "$BUILD_DIR/htmx.min.js" https://unpkg.com/htmx.org@1.9.9/dist/htmx.min.js
curl -Lfo "$BUILD_DIR/ws.min.js" https://unpkg.com/htmx.org@1.9.9/dist/ext/ws.js
curl -Lfo "$BUILD_DIR/chart.umd.min.js" https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js
curl -Lfo "$BUILD_DIR/luxon.min.js" https://cdnjs.cloudflare.com/ajax/libs/luxon/3.4.4/luxon.min.js
curl -Lfo "$BUILD_DIR/chartjs-adapter-luxon.umd.min.js" https://cdnjs.cloudflare.com/ajax/libs/chartjs-adapter-luxon/1.3.1/chartjs-adapter-luxon.umd.min.js

sha256sum -c << EOF
aa4f88f51685e7b22e9bd54b727d196168ee85b8ce5ce082c0197ec676976b5b  $BUILD_DIR/chartjs-adapter-luxon.umd.min.js
e9b0f875106021fb3d58120ad8ebdd3e7d32135a4452fd8918c72ac6475f2bd3  $BUILD_DIR/chart.umd.min.js
96a334a9570a382cf9c61a1f86d55870ba1c65e166cc5bcae98ddd8cdabeb886  $BUILD_DIR/htmx.min.js
ecd426d1b86f0c92a8b0bf1dfba6604a2d8bc59088700fd30f4f3b18b1013bd3  $BUILD_DIR/luxon.min.js
db6a44355128d5f795e8116df902dfe640b2598bbebd4fa655416b9fe734da14  $BUILD_DIR/ws.min.js
EOF

echo "Downloaded and verified."

cat \
	"$BUILD_DIR/chart.umd.min.js" <(echo) \
	"$BUILD_DIR/luxon.min.js" <(echo) \
	"$BUILD_DIR/chartjs-adapter-luxon.umd.min.js" <(echo) \
	"$BUILD_DIR/htmx.min.js" <(echo) \
	"$BUILD_DIR/ws.min.js" <(echo) \
	> vendor/vendor.js
