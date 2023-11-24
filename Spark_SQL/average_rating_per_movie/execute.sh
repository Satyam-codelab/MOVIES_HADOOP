if [ -d "result" ]; then
    echo "Removing previously existing results"
    rm -r "result"
else
    echo "Directory 'result' doesn't exist"
fi

echo "Executing script"
spark-shell -i average_rating_per_movie.scala

echo ""
echo "Contents of 'result' directory:"
echo ""
cat result/part*.csv
