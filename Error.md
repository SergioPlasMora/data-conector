Ps algo no funciono mismo tiempo (venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> python cli.py load-test --requests 1000 --concurrency 1000 --dataset dataset_1mb --tenants-list tenant_sergio
╭─────────────────────────────────╮
│ Arrow Flight over WebSocket PoC │
│ Multi-tenant Load Tester        │
╰─────────────────────────────────╯

Starting Load Test:
  Requests:    1000
  Concurrency: 1000
  Tenants:     1 (tenant_sergio...)
  Gateway:     grpc://localhost:8815
----------------------------------------

Load Test Results
        Summary Metrics        
╭────────────────┬────────────╮
│ Metric         │ Value      │
├────────────────┼────────────┤
│ Duration       │ 207.32 s   │
│ Total Requests │ 1000       │
│ Successful     │ 1000       │
│ Failed         │ 0          │
│ Throughput     │ 4.82 req/s │
│ Total Data     │ 896.94 MB  │
╰────────────────┴────────────╯
   Latency Statistics    
╭───────────┬───────────╮
│ Statistic │ Time (ms) │
├───────────┼───────────┤
│ Average   │ 206.57    │
│ P95       │ 225.00    │
╰───────────┴───────────╯
(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client>  y si se estan registrando las coneciones mira (venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\data-conector> python service.py --test
2025-12-11 13:59:12,537 - root - INFO - --- RUNNING IN TEST MODE (Console) ---
2025-12-11 13:59:12,537 - data_loader - INFO - Generating synthetic dataset with 1,000,000 rows...
2025-12-11 13:59:14,495 - data_loader - INFO - Dataset generated in 1.96s. Size: 61.99 MB
2025-12-11 13:59:14,557 - Connector - INFO - ArrowConnector initialized:
2025-12-11 13:59:14,557 - Connector - INFO -   Gateway URI: ws://localhost:8080/ws/connect
2025-12-11 13:59:14,557 - Connector - INFO -   Tenant ID: tenant_sergio
2025-12-11 13:59:14,558 - Connector - INFO -   Parallel Connections: 4
2025-12-11 13:59:14,560 - Connector - INFO - [Worker 0] Starting for tenant: tenant_sergio
2025-12-11 13:59:14,560 - Connector - INFO - [Worker 0] Connecting to ws://localhost:8080/ws/connect...
2025-12-11 13:59:14,616 - Connector - INFO - [Worker 1] Starting for tenant: tenant_sergio
2025-12-11 13:59:14,616 - Connector - INFO - [Worker 1] Connecting to ws://localhost:8080/ws/connect...
2025-12-11 13:59:14,617 - Connector - INFO - [Worker 2] Starting for tenant: tenant_sergio
2025-12-11 13:59:14,617 - Connector - INFO - [Worker 2] Connecting to ws://localhost:8080/ws/connect...
2025-12-11 13:59:14,618 - Connector - INFO - [Worker 3] Starting for tenant: tenant_sergio
2025-12-11 13:59:14,618 - Connector - INFO - [Worker 3] Connecting to ws://localhost:8080/ws/connect...
2025-12-11 13:59:14,684 - Connector - INFO - [Worker 2] Connected!
2025-12-11 13:59:14,684 - Connector - INFO - [Worker 1] Connected!
2025-12-11 13:59:14,686 - Connector - INFO - [Worker 3] Connected!
2025-12-11 13:59:14,687 - Connector - INFO - [Worker 0] Connected!
2025-12-11 13:59:14,690 - Connector - INFO - Registered successfully. Session: 55c61885-5d72-4861-ae13-3b21746eeee9
2025-12-11 13:59:14,692 - Connector - INFO - Registered successfully. Session: 11992e41-e987-4f2a-bee5-9bf441112f29
2025-12-11 13:59:14,694 - Connector - INFO - Registered successfully. Session: 3110138e-7a19-49d2-ab49-2ffaa1f1ec5a
2025-12-11 13:59:14,694 - Connector - INFO - Registered successfully. Session: bdc06b0d-0e1c-4e18-9328-e22c21134023 
INFO:     ('172.19.0.1', 46776) - "WebSocket /ws/connect" [accepted]

INFO:     connection open

INFO:     ('172.19.0.1', 46780) - "WebSocket /ws/connect" [accepted]

INFO:     connection open

INFO:     ('172.19.0.1', 46790) - "WebSocket /ws/connect" [accepted]

INFO:     connection open

INFO:     ('172.19.0.1', 46798) - "WebSocket /ws/connect" [accepted]

INFO:     connection open

2025-12-11 20:59:14,663 - app.websocket_manager - INFO - Registered tenant: tenant_sergio (Connection #1)

2025-12-11 20:59:14,664 - app.main - INFO - Connector registered: tenant_sergio

2025-12-11 20:59:14,665 - app.websocket_manager - INFO - Registered tenant: tenant_sergio (Connection #2)

2025-12-11 20:59:14,665 - app.main - INFO - Connector registered: tenant_sergio

2025-12-11 20:59:14,667 - app.websocket_manager - INFO - Registered tenant: tenant_sergio (Connection #3)

2025-12-11 20:59:14,667 - app.main - INFO - Connector registered: tenant_sergio

2025-12-11 20:59:14,667 - app.websocket_manager - INFO - Registered tenant: tenant_sergio (Connection #4)

2025-12-11 20:59:14,667 - app.main - INFO - Connector registered: tenant_sergio 
