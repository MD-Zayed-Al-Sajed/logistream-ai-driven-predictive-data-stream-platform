Domain-agnostic AIOps platform designed to ingest and analyze continuous, high-volume data streams from heterogeneous sources (Applications, Microservices, IoT, Incidents, and Logistics & Supply Chain). The system replaces traditional batch reporting with real-time, predictive intelligence.

# LogiStream AIOps Platform
AI-driven logistics event streaming and operations platform.

Monorepo layout aligned with:
- Kafka-based event bus
- Stream processing
- ML microservice
- Ops API gateway
- React dashboard
- Observability stack
---
<img width="1788" height="749" alt="Diagram" src="https://github.com/user-attachments/assets/b0eae0d7-e065-4fa5-8cb6-b809e7e7df4f" />
<img width="1463" height="912" alt="image" src="https://github.com/user-attachments/assets/99bfd8b4-99d0-4e19-a032-e44f1f767ded" />

---
Engineered an Event-Driven AIOps Platform: Built a high-concurrency system using Kafka and Faust to ingest and decode massive data streams from IoT, microservices, and logistics sources in real-time.

Implemented Predictive Analytics & ML: Replaced traditional batch reporting with in-flight feature engineering and machine learning to detect anomalies and predict logistics delays before they impact operations.

Designed a Distributed Real-Time Architecture: Leveraged TimescaleDB for high-velocity time-series data and Avro/Schema Registry to ensure data integrity across a distributed ecosystem of producers and consumers.

Developed Live Decision Dashboards: Created responsive React-based dashboards that consume live JSON streams via WebSockets, providing stakeholders with instant operational insights and automated system alerts.

Containerized Cloud-Native Deployment: Managed the full SDLC of a fully containerized environment, ensuring the platform is scalable, observable, and capable of handling heterogeneous data sources with sub-second latency.

## For Reliability
<img width="1557" height="668" alt="image" src="https://github.com/user-attachments/assets/673ccb55-1f7d-45e2-ab02-aebddf1a8085" />
<img width="1529" height="266" alt="image" src="https://github.com/user-attachments/assets/1d2b5ef9-4052-43d5-bf6c-c158e1f5288f" />




##Responsible Use Disclaimer

This project is a research-oriented AIOps system designed for:
- Data streaming  
- Predictive analytics  
- Educational and experimental purposes  
- Log & telemetry pipelines  

**It is NOT intended for safety-critical or regulated environments**, including but not limited to:

Medical systems  
Autonomous vehicle controls  
Nuclear systems  
Industrial robotics without safety review  
Life-critical infrastructure  

This platform uses:
- Heuristic logic  
- Non-deterministic AI components  
- Unverified statistical assumptions  

Use in production requires:
- Proper validation  
- Security review  
- Performance testing  
- Compliance checks  
The author assumes **no liability** for misuse.

## 📄 License
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Attribution Required](https://img.shields.io/badge/Attribution-Required-blue.svg)](NOTICE)

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

### 🏷 Attribution Requirement (Important)
This project includes original work authored by:

**© 2025 Md Zayed Al Sajed**
Any reuse, redistribution, or derivative work **MUST** include clear attribution:
> “Original author: Md Zayed Al Sajed — LogiStream AIOps Platform”

Attribution must appear in:
- Documentation  
- About screen / UI (if applicable)  
- README of derivative repos  
- Any public deployment or SaaS offering  

Failure to provide attribution **violates the NOTICE requirements**.
