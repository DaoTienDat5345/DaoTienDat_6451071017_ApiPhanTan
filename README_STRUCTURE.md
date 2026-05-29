# Cấu trúc đã tách theo service

```txt
DaoTienDat_6451071017_Webhook/
├── docker-compose.yml
├── package.json
├── .env
├── .env.example
├── webhook-service/
├── core-service/
├── backend-api/
└── retry-service/
```

## Chạy local từng service

```bash
npm --prefix webhook-service install
npm --prefix core-service install
npm --prefix backend-api install
npm --prefix retry-service install

npm run start:webhook
npm run start:core
npm run start:backend
npm run start:retry
```

## Chạy bằng Docker Compose

```bash
docker compose build
docker compose up -d
```

Kafka UI: http://localhost:8080
Webhook: http://localhost:3001/webhook
Backend API: http://localhost:3000
