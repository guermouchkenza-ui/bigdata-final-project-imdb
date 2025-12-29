# bigdata-final-project-imdb
Final project IMDB + Wikimedia streams
## Stream Processing – Wikimedia EventStreams

### Overview
This section implements a real-time stream processing pipeline using the Wikimedia EventStreams platform, which provides live edit events from Wikipedia pages.

### Tracked Entities
The following Wikipedia entities are tracked:
- United States  
- Donald Trump  
- Elon Musk  
- YouTube  
- Wikipedia  

### Metrics
For each entity, the system computes:
- Total edit count  
- Last edit timestamp  
- Last editor  
- Edit comment  

Metrics are stored in:
- `stream_output/metrics.csv`
- `stream_output/metrics_snapshot.json`

A snapshot is generated at startup to persist the system state even if no edits occur during the observation window.

### Alerting
An alerting mechanism logs abnormal situations such as connection errors or excessive activity in a time window. Alerts are written to `stream_output/alerts.log`.

### Notes
During execution, the Wikimedia streaming endpoint may return HTTP 403 responses, which can prevent live events from being ingested. This behavior is handled by the retry and alerting logic and reflects realistic constraints of real-time data pipelines.
