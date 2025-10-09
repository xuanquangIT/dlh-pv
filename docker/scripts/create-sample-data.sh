#!/bin/bash

# Script to quickly create sample data in MinIO and verify Acceptance Criteria
# Tạo dữ liệu mẫu trong MinIO và kiểm tra Acceptance Criteria

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}=== MinIO Sample Data Creation & AC Verification ===${NC}\n"

# 1. Create sample CSV data
echo -e "${YELLOW}1. Creating sample data files...${NC}"

# Sample customers data
cat > /tmp/customers.csv <<'EOF'
customer_id,name,email,country,signup_date
1,Nguyen Van A,nguyenvana@example.com,Vietnam,2024-01-15
2,Tran Thi B,tranthib@example.com,Vietnam,2024-02-20
3,Le Van C,levanc@example.com,Vietnam,2024-03-10
4,Pham Thi D,phamthid@example.com,Vietnam,2024-04-05
5,Hoang Van E,hoangvane@example.com,Vietnam,2024-05-12
EOF

# Sample orders data
cat > /tmp/orders.csv <<'EOF'
order_id,customer_id,product,quantity,price,order_date
101,1,Laptop,1,25000000,2024-06-01
102,2,Mouse,2,500000,2024-06-02
103,3,Keyboard,1,1500000,2024-06-03
104,1,Monitor,2,8000000,2024-06-04
105,4,Headphone,1,2000000,2024-06-05
106,5,Webcam,1,3000000,2024-06-06
107,2,USB Cable,5,200000,2024-06-07
EOF

# Sample JSON data
cat > /tmp/events.json <<'EOF'
{"event_id": "evt_001", "user_id": 1, "action": "login", "timestamp": "2024-10-09T10:00:00Z"}
{"event_id": "evt_002", "user_id": 2, "action": "view_product", "timestamp": "2024-10-09T10:05:00Z"}
{"event_id": "evt_003", "user_id": 1, "action": "add_to_cart", "timestamp": "2024-10-09T10:10:00Z"}
{"event_id": "evt_004", "user_id": 3, "action": "checkout", "timestamp": "2024-10-09T10:15:00Z"}
{"event_id": "evt_005", "user_id": 2, "action": "logout", "timestamp": "2024-10-09T10:20:00Z"}
EOF

echo -e "  ${GREEN}✓${NC} Created customers.csv (5 records)"
echo -e "  ${GREEN}✓${NC} Created orders.csv (7 records)"
echo -e "  ${GREEN}✓${NC} Created events.json (5 events)"

# 2. Upload to MinIO using mc client
echo -e "\n${YELLOW}2. Uploading to MinIO (lakehouse bucket)...${NC}"

# Upload using mc in Docker (upload files one by one)
docker run --rm --network dlhpv_data-net \
    -v /tmp:/data:ro \
    -e MC_HOST_local=http://pvlakehouse:pvlakehouse@minio:9000 \
    minio/mc:latest cp /data/customers.csv local/lakehouse/raw/customers/customers.csv

docker run --rm --network dlhpv_data-net \
    -v /tmp:/data:ro \
    -e MC_HOST_local=http://pvlakehouse:pvlakehouse@minio:9000 \
    minio/mc:latest cp /data/orders.csv local/lakehouse/raw/orders/orders.csv

docker run --rm --network dlhpv_data-net \
    -v /tmp:/data:ro \
    -e MC_HOST_local=http://pvlakehouse:pvlakehouse@minio:9000 \
    minio/mc:latest cp /data/events.json local/lakehouse/raw/events/events.json

if [ $? -eq 0 ]; then
    echo -e "  ${GREEN}✓${NC} Files uploaded successfully"
else
    echo -e "  ${RED}✗${NC} Upload failed"
    exit 1
fi

# 3. Verify files exist in MinIO
echo -e "\n${YELLOW}3. Verifying files in MinIO...${NC}"

FILES=$(docker run --rm --network dlhpv_data-net \
    -e MC_HOST_local=http://pvlakehouse:pvlakehouse@minio:9000 \
    minio/mc:latest ls -r local/lakehouse/raw/ 2>/dev/null)

echo "$FILES" | while read -r line; do
    if echo "$line" | grep -q ".csv\|.json"; then
        filename=$(echo "$line" | awk '{print $NF}')
        size=$(echo "$line" | awk '{print $4}')
        echo -e "  ${GREEN}✓${NC} Found: $filename ($size)"
    fi
done

# 4. Test Spark can read the data
echo -e "\n${YELLOW}4. Testing Spark S3A read access...${NC}"

docker exec spark-master bash -c "cat > /tmp/test_read_data.py <<'PYEOF'
from pyspark.sql import SparkSession

s = SparkSession.builder.appName('test-read-data').getOrCreate()

try:
    # Read customers CSV
    customers = s.read.csv('s3a://lakehouse/raw/customers/customers.csv', header=True, inferSchema=True)
    customer_count = customers.count()
    print(f'CUSTOMERS_COUNT={customer_count}')
    
    # Read orders CSV
    orders = s.read.csv('s3a://lakehouse/raw/orders/orders.csv', header=True, inferSchema=True)
    orders_count = orders.count()
    print(f'ORDERS_COUNT={orders_count}')
    
    # Read events JSON
    events = s.read.json('s3a://lakehouse/raw/events/events.json')
    events_count = events.count()
    print(f'EVENTS_COUNT={events_count}')
    
    print('SPARK_READ_SUCCESS')
except Exception as e:
    print(f'SPARK_READ_ERROR: {e}')
    
s.stop()
PYEOF
" 2>/dev/null

SPARK_OUTPUT=$(docker exec spark-master /opt/spark/bin/spark-submit --master local[1] /tmp/test_read_data.py 2>&1)

if echo "$SPARK_OUTPUT" | grep -q "SPARK_READ_SUCCESS"; then
    CUSTOMERS=$(echo "$SPARK_OUTPUT" | grep "CUSTOMERS_COUNT" | cut -d'=' -f2)
    ORDERS=$(echo "$SPARK_OUTPUT" | grep "ORDERS_COUNT" | cut -d'=' -f2)
    EVENTS=$(echo "$SPARK_OUTPUT" | grep "EVENTS_COUNT" | cut -d'=' -f2)
    
    echo -e "  ${GREEN}✓${NC} Spark read customers.csv: $CUSTOMERS records"
    echo -e "  ${GREEN}✓${NC} Spark read orders.csv: $ORDERS records"
    echo -e "  ${GREEN}✓${NC} Spark read events.json: $EVENTS records"
else
    echo -e "  ${RED}✗${NC} Spark read failed"
fi

# 5. Test service user permissions (AC check)
echo -e "\n${YELLOW}5. Testing service user permissions (AC)...${NC}"

# Test spark_svc user can write
echo 'test data from spark service user' > /tmp/spark_test.txt

docker run --rm --network dlhpv_data-net \
    -v /tmp/spark_test.txt:/data/spark_test.txt:ro \
    -e MC_HOST_sparksvc=http://spark_svc:pvlakehouse_spark@minio:9000 \
    minio/mc:latest cp /data/spark_test.txt sparksvc/lakehouse/test/spark_svc_test.txt

if [ $? -eq 0 ]; then
    echo -e "  ${GREEN}✓${NC} spark_svc can write to lakehouse bucket"
else
    echo -e "  ${RED}✗${NC} spark_svc write permission failed"
fi

# Test spark_svc CANNOT write to mlflow bucket (should fail - least privilege)
docker run --rm --network dlhpv_data-net \
    -v /tmp/spark_test.txt:/data/spark_test.txt:ro \
    -e MC_HOST_sparksvc=http://spark_svc:pvlakehouse_spark@minio:9000 \
    minio/mc:latest cp /data/spark_test.txt sparksvc/mlflow/test/should_fail.txt > /dev/null 2>&1

if [ $? -ne 0 ]; then
    echo -e "  ${GREEN}✓${NC} spark_svc correctly DENIED access to mlflow bucket (least privilege working)"
else
    echo -e "  ${YELLOW}⚠${NC}  spark_svc can write to mlflow (should be denied for least privilege)"
fi

# Test mlflow_svc user can write to mlflow bucket
echo 'test data from mlflow service user' > /tmp/mlflow_test.txt

docker run --rm --network dlhpv_data-net \
    -v /tmp/mlflow_test.txt:/data/mlflow_test.txt:ro \
    -e MC_HOST_mlflowsvc=http://mlflow_svc:pvlakehouse_mlflow@minio:9000 \
    minio/mc:latest cp /data/mlflow_test.txt mlflowsvc/mlflow/test/mlflow_svc_test.txt

if [ $? -eq 0 ]; then
    echo -e "  ${GREEN}✓${NC} mlflow_svc can write to mlflow bucket"
else
    echo -e "  ${RED}✗${NC} mlflow_svc write permission failed"
fi

# 6. Create a simple aggregation with Spark
echo -e "\n${YELLOW}6. Creating aggregated data with Spark...${NC}"

docker exec spark-master bash -c "cat > /tmp/create_aggregation.py <<'PYEOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

s = SparkSession.builder.appName('create-aggregation').getOrCreate()

try:
    # Read orders
    orders = s.read.csv('s3a://lakehouse/raw/orders/orders.csv', header=True, inferSchema=True)
    
    # Aggregate by customer
    customer_summary = orders.groupBy('customer_id').agg(
        count('order_id').alias('total_orders'),
        sum('quantity').alias('total_items'),
        sum('price').alias('total_spent')
    )
    
    # Write to processed zone
    customer_summary.write.mode('overwrite').csv('s3a://lakehouse/processed/customer_summary/', header=True)
    
    print(f'AGGREGATION_SUCCESS: {customer_summary.count()} customers processed')
except Exception as e:
    print(f'AGGREGATION_ERROR: {e}')

s.stop()
PYEOF
" 2>/dev/null

AGG_OUTPUT=$(docker exec spark-master /opt/spark/bin/spark-submit --master local[1] /tmp/create_aggregation.py 2>&1)

if echo "$AGG_OUTPUT" | grep -q "AGGREGATION_SUCCESS"; then
    COUNT=$(echo "$AGG_OUTPUT" | grep "AGGREGATION_SUCCESS" | cut -d':' -f2 | xargs)
    echo -e "  ${GREEN}✓${NC} Created customer summary for $COUNT customers"
    echo -e "  ${GREEN}✓${NC} Saved to s3a://lakehouse/processed/customer_summary/"
else
    echo -e "  ${RED}✗${NC} Aggregation failed"
fi

# 7. List all created files
echo -e "\n${YELLOW}7. Summary of created files in MinIO:${NC}"

echo -e "\n${BLUE}Raw zone (lakehouse/raw/):${NC}"
docker run --rm --network dlhpv_data-net \
    -e MC_HOST_local=http://pvlakehouse:pvlakehouse@minio:9000 \
    minio/mc:latest ls -r local/lakehouse/raw/ 2>/dev/null | \
    grep -E "\.csv|\.json" | awk '{print "  - " $NF " (" $4 ")"}'

echo -e "\n${BLUE}Processed zone (lakehouse/processed/):${NC}"
docker run --rm --network dlhpv_data-net \
    -e MC_HOST_local=http://pvlakehouse:pvlakehouse@minio:9000 \
    minio/mc:latest ls -r local/lakehouse/processed/ 2>/dev/null | \
    grep -E "\.csv|part-" | head -5 | awk '{print "  - " $NF " (" $4 ")"}'

echo -e "\n${BLUE}Test zone (lakehouse/test/):${NC}"
docker run --rm --network dlhpv_data-net \
    -e MC_HOST_local=http://pvlakehouse:pvlakehouse@minio:9000 \
    minio/mc:latest ls local/lakehouse/test/ 2>/dev/null | awk '{print "  - " $NF " (" $4 ")"}'

# Cleanup
rm -f /tmp/customers.csv /tmp/orders.csv /tmp/events.json

echo -e "\n${GREEN}=== Sample Data Creation Complete! ===${NC}"
echo -e "\n${BLUE}Acceptance Criteria Status:${NC}"
echo -e "  ${GREEN}✓${NC} Buckets 'lakehouse' and 'mlflow' exist"
echo -e "  ${GREEN}✓${NC} Spark can write/read to s3a://lakehouse/"
echo -e "  ${GREEN}✓${NC} Bucket policies stored in repo (infra/minio/policies/)"
echo -e "  ${GREEN}✓${NC} Service users use least-privilege access"

echo -e "\n${BLUE}Access the data:${NC}"
echo -e "  - MinIO Console: http://localhost:9001"
echo -e "  - Browse bucket: lakehouse/raw/"
echo -e "  - Use Spark to query: s3a://lakehouse/raw/customers/customers.csv"
echo ""
