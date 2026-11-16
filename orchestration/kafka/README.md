# WS3 -- Kafka replay broker

One KRaft broker, single-partition topics, loopback only. It exists to replay
one quarter through `pit.tools.KafkaReplayMain` and `pit.tools.KafkaLandingMain`
(both test scope). Decision record: `docs/adr/0003-kafka-ingest-route.md`. Run
record: `docs/KAFKA-INGEST-EVIDENCE.md`.

```bash
cd orchestration/kafka
docker compose up -d --wait
docker compose ps                      # kafka: healthy
# MSYS_NO_PATHCONV=1 stops Git Bash rewriting /opt/... into a Windows path
# before docker sees it; other POSIX shells ignore it.
MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic vantage.num.v1 --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
docker compose down                    # drops the topic data with the container
```

Port 9092 must be free on the host. Override the image with
`VANTAGE_KAFKA_IMAGE` if needed; there is nothing else to configure.
