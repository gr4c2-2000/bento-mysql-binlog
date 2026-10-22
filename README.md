# bento-mysql-binlog

Wtyczka wejściowa dla [Bento](https://github.com/warpstreamlabs/bento), która
czyta binlog MySQL przez [go-mysql](https://github.com/go-mysql-org/go-mysql)
i podaje zmiany wierszy dalej **partiami całych transakcji**, nie po jednym
zdarzeniu.

W repo jest też kompletny PoC: MySQL z włączonym binlogiem, Kafka, RabbitMQ
i router Bento, który jeden strumień CDC rozsyła na oba sinki z osobnymi
filtrami.

```
                      ┌─ table == orders ──────► Kafka  shop.orders
MySQL binlog ─► Bento ┤
                      └─ action == delete ─────► RabbitMQ shop.events → shop.deletes
```

## Szybki start

```bash
make up      # mysql + kafka + rabbitmq + bento
make smoke   # end-to-end: zapis do MySQL, asercje na obu sinkach
make down    # sprzątanie razem z wolumenami
```

`make smoke` wpisuje do MySQL osiem zmian wierszy i sprawdza, że Kafka dostała
dokładnie pięć (same `shop.orders`), a RabbitMQ dokładnie dwie (same `delete`).
Jedno zdarzenie — usunięcie zamówienia — trafia do obu, bo filtry się nakładają.

## Wejście `mysql_binlog`

```yaml
input:
  mysql_binlog:
    addr: mysql:3306
    user: repl
    password: "${MYSQL_REPL_PASSWORD}"
    server_id: 1001
    include_tables: [ 'shop\..*' ]
    position_file: /var/lib/bento/binlog.pos
    max_batch_rows: 100
    max_batch_wait: 500ms
```

| Pole | Domyślnie | Znaczenie |
|---|---|---|
| `addr` | `127.0.0.1:3306` | host i port serwera |
| `user`, `password` | — | konto z `REPLICATION SLAVE` i `REPLICATION CLIENT` |
| `flavor` | `mysql` | `mysql` albo `mariadb` |
| `server_id` | `1001` | identyfikator repliki, unikalny w całej topologii |
| `include_tables` | wszystkie | regexpy `schemat.tabela` |
| `exclude_tables` | brak | odejmowane od `include_tables` |
| `start_from` | `saved` | `saved`, `current` albo `earliest` |
| `position_file` | `./binlog.pos` | plik z ostatnią dostarczoną pozycją |
| `max_batch_rows` | `500` | górny limit wierszy w partii |
| `max_batch_wait` | `200ms` | ile czekać na kolejne transakcje |
| `max_transaction_rows` | `10000` | bezpiecznik dla ogromnych transakcji |

Payload wiadomości:

```json
{
  "schema": "shop",
  "table": "orders",
  "action": "update",
  "before": { "id": 1, "status": "new",  "amount": "100.00" },
  "after":  { "id": 1, "status": "paid", "amount": "100.00" },
  "file": "mysql-bin.000003",
  "pos": 4821,
  "ts_unix": 1761000000
}
```

Metadane — `db`, `table`, `action`, `binlog_file`, `binlog_pos` — pozwalają
filtrować bez parsowania payloadu: `meta("table") != "orders"`.

## Dlaczego partiami

Canal woła handler osobno dla każdego zdarzenia wiersza. Oddawanie Bento
jednej wiadomości naraz oznacza jedno potwierdzenie i jeden zapis offsetu na
wiersz, a po stronie Kafki — rundę na wiersz zamiast jednego produce'a na
partię.

Wtyczka buforuje wiersze do zdarzenia commitu (`XID`) i dopiero wtedy wypuszcza
transakcję dalej. `ReadBatch` skleja kolejne transakcje aż do `max_batch_rows`
albo do upływu `max_batch_wait`. Z tego wynikają dwie własności:

- **transakcja nigdy nie jest dzielona** między partie, więc partia może
  nieznacznie przekroczyć `max_batch_rows`;
- **każda commitowalna pozycja to granica transakcji**, nigdy jej środek.

Wyjątkiem jest transakcja większa niż `max_transaction_rows` — jest zrzucana
wcześniej, z pozycją ze środka transakcji, i wtyczka loguje to ostrzeżeniem.

## Offsety i gwarancje

Dostarczanie jest **at-least-once**. Pozycja binloga jest zapisywana dopiero,
gdy Bento potwierdzi partię, czyli gdy przyjmą ją wszystkie wyjścia brokera
`fan_out`. Nack nie przesuwa offsetu — partia zostaje powtórzona. Sinki muszą
być idempotentne.

Potwierdzenia nie wracają w kolejności odczytu, więc wtyczka nie zapisuje
„ostatniego potwierdzonego", tylko **watermark**: pozycję najnowszej partii,
której wszystkie poprzedniczki też są potwierdzone (`internal/binlog/offsets.go`).
Bez tego awaria po potwierdzeniu partii nr 3, gdy nr 2 jeszcze leci, przeskoczyłaby
wiersze z partii nr 2.

Zapis pliku pozycji idzie przez plik tymczasowy i `rename`, więc crash w trakcie
nie zostawi obciętego offsetu.

## Wymagania po stronie MySQL

```sql
-- binlog_format = ROW, binlog_row_image = FULL
CREATE USER 'repl'@'%' IDENTIFIED BY '...';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'repl'@'%';
```

`SELECT` jest potrzebny, bo canal czyta `information_schema`, żeby mapować
pozycje kolumn na nazwy. Bez `binlog_row_image=FULL` obraz `before` przy
`UPDATE` i `DELETE` będzie niekompletny.

## Ograniczenia

- **Jeden proces na źródło.** Jeden `server_id`, jeden czytnik. Dwa procesy z tym
  samym identyfikatorem rozłączają się nawzajem, a z różnymi — dublują strumień.
  Równoległość zaczyna się za Bento (partycje Kafki).
- **Brak snapshotu.** To czysty tail binloga; `mysqldump` jest wyłączony celowo.
  Backfill istniejących danych trzeba zrobić osobno.
- **Zdarzenia DDL nie są emitowane.** Canal sam odświeża cache schematu, ale
  zmiany struktury nie trafiają na strumień.
- **Pozycja stoi, gdy nie ma pasującego ruchu.** Jeśli `include_tables` odfiltruje
  cały ruch, offset się nie przesuwa i restart przewija binlog od ostatniego
  pasującego zdarzenia.
- **Plik pozycji jest lokalny.** Przy odtwarzaniu kontenera musi przetrwać na
  wolumenie, inaczej `start_from: saved` zdegraduje się do `current`.

## Rozwój

```bash
make test    # testy jednostkowe
make race    # to samo z wykrywaniem wyścigów
make lint    # go vet + bento lint na konfiguracji routera
```

Testy integracyjne wymagają działającego MySQL ze stacka:

```bash
docker compose -f deploy/docker-compose.yml up -d --wait mysql
go test -tags integration ./test/...
```

Układ repo:

```
cmd/bento-mysql/      dystrybucja Bento z wkompilowaną wtyczką
internal/binlog/      event.go  konwersja wierszy binloga
                      handler.go buforowanie transakcji z callbacków canala
                      input.go   batching i ReadBatch
                      offsets.go watermark potwierdzeń
                      store.go   trwała pozycja
deploy/               docker-compose, init MySQL, definicje RabbitMQ, router Bento
scripts/smoke.sh      test end-to-end całego stacka
test/                 testy integracyjne (build tag `integration`)
```
