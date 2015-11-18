SPARK_PATH="/Users/shanmukh/Documents/apps/spark/bin/"
MASTER=local[*]
JARPATH="/Users/shanmukh/Documents/Projects/YelpChallange/out/artifacts/Task2_jar/YelpChallenge.jar"
pushd $SPARK_PATH
./spark-submit --master $MASTER $JARPATH
popd