(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> python cli.py query --tenant tenant_sergio --dataset dataset_10mb
╭─────────────────────────────────╮
│ Arrow Flight over WebSocket PoC │
│ Multi-tenant Load Tester        │
╰─────────────────────────────────╯

Running single query for tenant: tenant_sergio, dataset: dataset_10mb, rows: Default     

  Metric             Value         
 ──────────────────────────────────
  Status             Success
  Tenant             tenant_sergio
  Rows               214,999
  Bytes              8.76 MB
  Metadata Latency   169.05 ms
  Transfer Latency   485.60 ms
  Total Latency      654.65 ms

(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> python cli.py query --tenant tenant_sergio --dataset dataset_50mb
╭─────────────────────────────────╮
│ Arrow Flight over WebSocket PoC │
│ Multi-tenant Load Tester        │
╰─────────────────────────────────╯

Running single query for tenant: tenant_sergio, dataset: dataset_50mb, rows: Default

  Metric             Value          
 ────────────────────────────────── 
  Status             Success        
  Tenant             tenant_sergio  
  Rows               1,060,999      
  Bytes              43.26 MB       
  Metadata Latency   185.39 ms
  Transfer Latency   2574.19 ms
  Total Latency      2759.58 ms

(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> python cli.py query --tenant tenant_sergio --dataset dataset_100mb
╭─────────────────────────────────╮
│ Arrow Flight over WebSocket PoC │
│ Multi-tenant Load Tester        │
╰─────────────────────────────────╯

Running single query for tenant: tenant_sergio, dataset: dataset_100mb, rows: Default    

  Metric             Value         
 ──────────────────────────────────
  Status             Success
  Tenant             tenant_sergio
  Rows               2,101,999
  Bytes              85.60 MB
  Metadata Latency   258.07 ms
  Transfer Latency   4784.58 ms
  Total Latency      5042.65 ms

(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> 

(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> python cli.py query --tenant tenant_sergio --dataset dataset_1kb  
╭─────────────────────────────────╮
│ Arrow Flight over WebSocket PoC │
│ Multi-tenant Load Tester        │
╰─────────────────────────────────╯

Running single query for tenant: tenant_sergio, dataset: dataset_1kb, rows: Default      

  Metric             Value         
 ──────────────────────────────────
  Status             Success
  Tenant             tenant_sergio
  Rows               3
  Bytes              0.00 MB
  Metadata Latency   116.75 ms
  Transfer Latency   67.89 ms
  Total Latency      184.64 ms

(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> 

(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> python cli.py load-test --requests 100 --concurrency 50 --dataset dataset_1kb --tenants-list tenant_sergio 
╭─────────────────────────────────╮
│ Arrow Flight over WebSocket PoC │
│ Multi-tenant Load Tester        │
╰─────────────────────────────────╯

Starting Load Test:
  Requests:    100
  Concurrency: 50
  Tenants:     1 (tenant_sergio...)
  Gateway:     grpc://localhost:8815
----------------------------------------

Load Test Results
        Summary Metrics        
╭────────────────┬────────────╮
│ Metric         │ Value      │
├────────────────┼────────────┤
│ Duration       │ 20.19 s    │
│ Total Requests │ 100        │
│ Successful     │ 100        │
│ Failed         │ 0          │
│ Throughput     │ 4.95 req/s │
│ Total Data     │ 0.03 MB    │
╰────────────────┴────────────╯
   Latency Statistics    
╭───────────┬───────────╮
│ Statistic │ Time (ms) │
├───────────┼───────────┤
│ Average   │ 201.17    │
│ P95       │ 207.95    │
╰───────────┴───────────╯
(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client>

(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> python cli.py load-test --requests 100 --concurrency 50 --dataset dataset_10mb --tenants-list tenant_sergio       
╭─────────────────────────────────╮
│ Arrow Flight over WebSocket PoC │
│ Multi-tenant Load Tester        │
╰─────────────────────────────────╯

Starting Load Test:
  Requests:    100
  Concurrency: 50
  Tenants:     1 (tenant_sergio...)
  Gateway:     grpc://localhost:8815
----------------------------------------

Load Test Results
        Summary Metrics        
╭────────────────┬────────────╮
│ Metric         │ Value      │
├────────────────┼────────────┤
│ Duration       │ 60.82 s    │
│ Total Requests │ 100        │
│ Successful     │ 100        │
│ Failed         │ 0          │
│ Throughput     │ 1.64 req/s │
│ Total Data     │ 876.49 MB  │
╰────────────────┴────────────╯
   Latency Statistics    
╭───────────┬───────────╮
│ Statistic │ Time (ms) │
├───────────┼───────────┤
│ Average   │ 606.34    │
│ P95       │ 702.06    │
╰───────────┴───────────╯
(venv) PS C:\Users\sergi\OneDrive\Documentos\GitHub\cli-client> 