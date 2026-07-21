#!/bin/sh

set -e

echo "Waiting for Elasticsearch..."

until curl -s http://elasticsearch:9200 >/dev/null
do
    sleep 2
done

if curl -s -o /dev/null -w "%{http_code}" http://elasticsearch:9200/logs | grep -q "200"; then
    echo "Index logs already exists."
else
    echo "Creating logs index..."

    curl -X PUT "http://elasticsearch:9200/logs" \
         -H "Content-Type: application/json" \
         -d '{
           "settings": {
             "number_of_shards": 1,
             "number_of_replicas": 0
           }
         }'
fi

echo "Elasticsearch initialization complete."