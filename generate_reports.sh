#!/usr/bin/env bash

#nhoods=`psql -X -A -d blight_data -U postgres -h caeser-midt.memphis.edu -t -c "select name from geography.clean_memphis;"`
nhoods=($psql -X -A -d blight_data -U postgres -h caeser-midt.memphis.edu -t -c "select name from geography.clean_memphis;")

# where substring(name, 1,1) = 'E';"`
#echo "$nhoods"
#dir = /home/nate/source/midt/nbhood_reports.py
for nhood in ${nhoods}; do
    echo "${nhood}"
    #/usr/bin/python nbhood_reports.py neighborhood "${nhood}"
done
