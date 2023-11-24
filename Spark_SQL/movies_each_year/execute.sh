#!/usr/bin/env bash

if [ -d "result" ]; then
    echo "Removing previous results"
    echo ""
    rm -r "result"
else
    echo "Directory 'result' doesn't exist"
    echo ""
fi

echo "Executing script"
echo ""
spark-shell -i no_of_movies_each_year.scala

echo ""
echo "The Number of movies for each year"
echo ""
cat result/part*.csv
echo ""
