# Architecture Notes: Large-Scale Video Analytics Architecture

## Pipeline

```text
RTSP Cameras -> Stream Ingestion -> Kafka -> GPU Workers -> Detection -> Tracking -> Action Recognition -> Database -> Analytics API -> Dashboard
```

## Components

- RTSP stream ingestion (simulated at scale)
- Message queue-based buffering (Kafka)
- GPU worker pool for detection/tracking
- Action recognition stage
- Centralized analytics database
- Analytics API
- Monitoring dashboard
- Horizontal scaling demonstration (10 -> 100 -> 1000 streams)

## Design Notes

- Keep provider/model choices swappable behind interfaces (see `multi-llm-router`
  and similar projects in this portfolio for the general pattern).
- Prefer configuration-driven pipelines (YAML/JSON in `configs/`) over hardcoded
  parameters so experiments are reproducible.
