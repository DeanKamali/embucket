# State Store

Utilities and models for persisting Embucket state (sessions, query history)
into a durable store. Backends:

- **Postgres** (`PostgresStateStore`) — the durable backend for real deployments
- **In-memory** (`InMemoryStateStore`) — tests and local development only

## Local Postgres setup

```bash
docker run -d --name embucket-statestore-pg -p 5439:5432 \
  -e POSTGRES_USER=embucket -e POSTGRES_PASSWORD=embucket \
  -e POSTGRES_DB=embucket postgres:17
```

The schema (`state_sessions`, `state_queries` tables) is created automatically
on first use.

### Environment variables

```bash
STATESTORE_PG_URL=postgres://embucket:embucket@localhost:5439/embucket
# Falls back to DATABASE_URL when unset
```
