1) Поднимаем TEI на серваке:

```bash
sudo docker run --gpus all \
  -p 8080:80 \
  -v ~/.cache/huggingface:/root/.cache/huggingface \
  -e MODEL_ID=deepvk/USER-bge-m3 \
  ghcr.io/huggingface/text-embeddings-inference:latest

```
- `ssh -L 8080:localhost:8080 workgpu` - проброс
- `curl -v http://localhost:8080/health` - проверка

2) Поднимаем qdrant:

```bash
docker run -p 6333:6333 -p 6334:6334 \
    -v "$(pwd)/qdrant_storage:/qdrant/storage:z" \
    qdrant/qdrant
```
- `ssh -L 6333:localhost:6333 workgpu` - проброс
- `http://localhost:6333/dashboard` - дашборд

## Kafka (локальная разработка)

- Поднять весь стек: `docker-compose up -d redpanda kafka-ui redis qdrant app kafka-worker`
- Базовые переменные в `.env` (пример):
  - `KAFKA_BOOTSTRAP_SERVERS=redpanda:9092`
  - `KAFKA_INPUT_TOPIC=gadalka-input`
  - `KAFKA_OUTPUT_TOPIC=gadalka-output`
  - `KAFKA_GROUP_ID=gadalka-consumer`
  - `MESSAGE_BUS_MODE=kafka`
- Запуск воркера вручную: `python -m kafka_bus.worker`

### Контракт сообщений

- input_topic payload:
  ```json
  {
    "request_text": "..."
  }
  ```
  headers:
  - `request_id`
  - `bot_id` (используется как user_id)
  - `chat_id` (используется как session_id)
  - `natal_chart` (строка или JSON)

- output_topic payload:
  ```json
  {
    "response_text": "..."
  }
  ```
  headers: `request_id`, `bot_id`, `chat_id`

## Mock режим (без Kafka)

- Установить `MESSAGE_BUS_MODE=mock`
- Запускать воркер локально без брокера: `MESSAGE_BUS_MODE=mock python -m kafka_bus.worker`
- Можно засунуть сообщение в mock: `bus.enqueue_input({'message': 'hi'})` и получить результат через `bus.get_topic_messages(output_topic)` (вернёт список dict с `value` и `headers`).

## Railway

- Основные переменные окружения:
  - `MESSAGE_BUS_MODE=kafka`
  - `KAFKA_BOOTSTRAP_SERVERS` (из Railway)
  - `KAFKA_GROUP_ID`
  - `KAFKA_INPUT_TOPIC`
  - `KAFKA_OUTPUT_TOPIC`
  - `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_SASL_MECHANISM`, `KAFKA_SECURITY_PROTOCOL` (если кластер требует SASL)
- Команда запуска воркера: `python -m kafka_bus.worker`
- Команда запуска API: `python src/main.py --host 0.0.0.0 --port 8000`
