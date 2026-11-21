set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOAD_DATE=""
LOG_FILE="checks.log"

while [[ $# -gt 0 ]]; do
    case "$1" in 
        --load-date)
            LOAD_DATE="$2"
            shift 2
            ;;
        --log-file)
            LOG_FILE="$2"
            shift 2
            ;;
        *)
            echo "Неизвестный аргумент: $1"
            exit 1
            ;;
    esac
done

if [[ -z "$LOAD_DATE" ]]; then
    echo "Необходимо указать --load-date"
    exit 1
fi

if [[ "$LOG_FILE" != /* ]]; then
    LOG_FILE="$SCRIPT_DIR/$LOG_FILE"
fi

RUN_ID="$(date +%Y%m%d_%H%M%S)_$$"

exec > >(tee -a "$LOG_FILE") 2>&1
trap 'code=$?; echo "RUN_ID=$RUN_ID LOAD_DATE=$LOAD_DATE EXIT_CODE=$code"' EXIT


echo "Запуск RAW-ORDERS"
bash "$PROJECT_DIR/spark/raw/orders/orders.sh" --load-date "$LOAD_DATE"

echo "Запуск RAW-PAYMENTS"
bash "$PROJECT_DIR/spark/raw/payments/payments.sh" --load-date "$LOAD_DATE"

echo "Запуск STG-ORDERS"
bash "$PROJECT_DIR/spark/staging/stg_orders/stg_orders.sh" --load-date "$LOAD_DATE"

echo "Запуск STG-PAYMENTS"
bash "$PROJECT_DIR/spark/staging/stg_payments/stg_payments.sh" --load-date "$LOAD_DATE"

echo "Запуск INT-ORDERS-WITH-PAYMENTS"
bash "$PROJECT_DIR/spark/intermediate/int_orders_with_payments/int_orders_with_payments.sh" --load-date "$LOAD_DATE"

echo "Запуск FCT-ORDERS"
bash "$PROJECT_DIR/spark/marts/fct_orders/fct_orders.sh" --load-date "$LOAD_DATE"


echo "Запуск check.py"

export PYTHONPATH="$PROJECT_DIR/spark${PYTHONPATH:+:$PYTHONPATH}"

spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name pipeline-check \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.driver.memory=1g \
    --conf spark.dynamicAllocation.enabled=false \
    "$SCRIPT_DIR/check.py" --load-date "$LOAD_DATE"