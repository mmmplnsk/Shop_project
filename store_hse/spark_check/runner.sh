 set -e


 SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

 for LOAD_DATE in 2026-03-05 2026-03-06; do
    echo "Запуск с LOAD DATE = $LOAD_DATE"

    bash "$SCRIPT_DIR/run_check.sh" \
         --load-date "$LOAD_DATE" \
         --log-file checks.log

done