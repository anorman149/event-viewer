# Mission

Event Viewer is a real-time event observability platform that empowers teams to visualize, analyze, and act on high-volume event streams.

## Problem

Modern distributed systems generate massive, continuous streams of events. Without the right tooling, these streams are invisible noise — operators miss anomalies, analysts wait hours for answers, and engineers spend days building one-off scripts just to ask basic questions of their data.

## Solution

Event Viewer provides a single, purpose-built platform where any team can:

- **Ingest** events at scale (target: 1 million events/second) from any source or format
- **Explore** events through full-text search, structured filters, and time-range queries
- **Visualize** patterns and trends in interactive, customizable dashboards
- **Act** on critical signals through real-time alerts and notifications

## Target Audience

| Persona | Primary Need |
|---|---|
| Developers & DevOps | Monitor and troubleshoot event-driven systems |
| Data Analysts & BI | Understand event patterns and produce reports |
| Security Professionals | Detect and investigate security-related events |
| System Administrators | Optimize and maintain event-driven infrastructure |
| Data Scientists | Analyze event data for insights and model inputs |

## Success Criteria

- Sustain 1M+ events/second ingest without data loss
- Sub-second query response for filtered event searches
- Dashboard load time under 2 seconds
- 99.9% uptime for the ingest pipeline
- Schema registry supports extensible, versioned event types
- Full RBAC, audit logging, and data retention controls for compliance