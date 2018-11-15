#!/usr/bin/env bash
#host=localhost
host=caeser-midt.memphis.edu
origin=$1
if [ -z "$origin" ]; then
    origin="Clean Memphis"
fi

nhoods=`psql -A -d blight_data -U postgres -h ${host} -t -c "select name from geography.boundaries where origin = '${origin}';"`
IFS=$'\n'
for nhood in ${nhoods}; do
    echo
    echo "Creating report for ${nhood}"
    echo
    /usr/bin/python nbhood_reports.py neighborhood ${nhood}
done
