CARA JALAN

# 1) Ondemand
bash tests/ondemand_test.sh

# 2) Functional
bash tests/functional_test.sh

# 3) Connection
ENDPOINTS="idx.co.id,example.com" bash tests/connection_test.sh

# 4) Integration (end-to-end)
bash tests/integration_test.sh
