#!/bin/bash

BUCKET_NAME="<YOUR_BUCKET_NAME>"
FILES=(
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet"
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet"
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-04.parquet"
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet"
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-06.parquet"
)

for FILE in "${FILES[@]}"; do
  wget $FILE
  FILENAME=$(basename $FILE)
  gcloud storage cp $FILENAME gs://$BUCKET_NAME/
done

gcloud storage ls gs://$BUCKET_NAME/

# Run this script from the terminal using:
# chmod +x upload_parquet.sh
# ./upload_parquet.sh

