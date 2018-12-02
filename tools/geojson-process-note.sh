#!/bin/bash

# Reduce precision of the decimal value, limited to 5 places
# https://www.npmjs.com/package/geojson-precision
geojson-precision -p 5 1051.geojson rp-1051.geojson

# https://mapshaper.org/ to reduce points to 23.5%
mapshaper rp-1051.geojson\
  -simplify percentage=23.5% visvalingam weighted keep-shapes stats\
  -o format=geojson ms-rp-1051.geojson

# Produce newline-delimited JSON with each feature as a line.
cat ms-rp-1051.geojson | jq -c '.features | .[]' > ms-rp-1051.jsonld

# Import to MongoDB
mongoimport --db ctp -c ForestCompartmentBoundary --file "ms-rp-1051.jsonld"


## articles
# http://zevross.com/blog/2014/04/22/spatial-data-on-a-diet-tips-for-file-size-reduction-using-topojson/
# https://gis.stackexchange.com/questions/81508/leaflet-geojson-webpage-performance-out-of-hand-how-to-convert-to-png
# http://blog.mastermaps.com/2013/06/converting-shapefiles-to-topojson.html
# https://github.com/topojson/topojson
# Tidying up geoJSON - doesn't seem to work.
# https://github.com/mapbox/geojson-tidy

# Organise each feature JSON object into a single line.
# https://stackoverflow.com/questions/22029114/how-to-import-geojson-file-to-mongodb
# jq --compact-output ".features" rp-1051.geojson > rp-ex-1051.geojson

# https://github.com/topojson/topojson-server/blob/master/README.md#geo2topo
# geo2topo -n rp-ex-1051.geojson > rp-ex-1051.topojson

# Read each line and convert it to topojson individually
# while IFS='' read -r line || [[ -n "$line" ]]; do
#   geo2topo "$line" | cat > rp-1051.topojson
# done < "$1"

# useful tools: jq, jsonfui, mapshaper, geojson-precision, topojson-server 