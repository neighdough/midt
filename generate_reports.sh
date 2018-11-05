#!/usr/bin/env bash
host=localhost
#host=caeser-midt.memphis.edu
nhoods=`psql -A -d blight_data -U postgres -h ${host} -t -c "select name from geography.clean_memphis where substring(name, 1, 1) = 'U';"`
IFS=$'\n'
for nhood in ${nhoods}; do
    echo "${nhood}"
    /usr/bin/python nbhood_reports.py neighborhood \"${nhood}\"
done
