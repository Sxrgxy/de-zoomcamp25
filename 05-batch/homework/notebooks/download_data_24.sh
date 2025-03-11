#bin/bash

set -e # Exit on error

TAXI_TYPE=$1 #'yellow'
YEAR=$2 #2020
MONTH=${3:-10} # Default to October if month not provided

#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"


FMONTH=`printf "%02d" ${MONTH}`
#wget -O data/$TAXI_TYPE-$YEAR-$MONTH.csv https://s3.amazonaws.com/nyc-tlc/trip+data/$TAXI_TYPE_tripdata_$YEAR-$FMONTH.csv
URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

LOCAL_PREFIX="data/pq/${TAXI_TYPE}/${YEAR}/${FMONTH}"
LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

echo "downloading ${URL} to ${LOCAL_PATH}"
mkdir -p ${LOCAL_PREFIX}
wget ${URL} -O ${LOCAL_PATH}

