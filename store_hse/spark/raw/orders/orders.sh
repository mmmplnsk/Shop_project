SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="$(cd "$SCRIPT_DIR/../.." && pwd)${PYTHONPATH:+:$PYTHONPATH}"

spark-submit \
    --master spark://spark-master:7077 \
    --name raw-orders \
    --deploy-mode client \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.driver.memory=1g \
    --conf spark.dynamicAllocation.enabled=false \
    "$SCRIPT_DIR/orders.py" "$@"