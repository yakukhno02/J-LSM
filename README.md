# J-LSM: Log-Structured Merge Key-Value Storage Engine

Автономна вбудована бібліотека сховища ключ-значення на базі LSM-дерева, написана на чистій Java 21 (`java.base`, без сторонніх runtime-залежностей).

---

## 👥 Команда розробки

| Роль | Розробник | Зона відповідальності | Пакети |
| :--- | :--- | :--- | :--- |
| **Dev 1** | Maria Yakukhno | RAM, WAL, Recovery, Manifest, Engine Architecture | `api`, `mem`, `wal`, `manifest`, `engine` |
| **Dev 2** | Nazar Tsebrynskyi | Disk I/O, SSTable Codecs, K-Way Merge, Compactor | `internal.io`, `sst`, `merge`, `compaction` |

---

## 📚 Документація

- [**DESIGN.md**](docs/DESIGN.md) — Архітектурний дизайн-документ (структура, формати файлів, життєвий цикл).
- [**DEVELOPMENT.md**](docs/DEVELOPMENT.md) — Специфікація розробки, спільні контракти та матриця відповідальності.

---

## 🚀 Основні можливості

- **Append-only WAL** із захистом цілісності через CRC32, підтримкою атомарних пакетів (`0x03 BATCH`) та діапазонних видалень (`0x04 RANGE_DELETE`), а також автоматичним відновленням після обривів запису (torn-write recovery).
- **Дворівневий MVCC MemTable:** активна таблиця для запису та незмінна (immutable) для фонового скидання (flush) із підтримкою багатоверсійної історії та інтервальних томбстоунів (`RangeTombstone`).
- **Формат SSTable:** вирівняні 4 КБ Data Blocks, вбудований бітовий **Bloom Filter** (`JBLM`, $m/n = 10$, $k = 7$, Murmur3 64-bit double hashing), бінарний Sparse Index та перевірка Magic Number у Footer.
- **Розширений Compactor (SSTable Splitting, STCS & Leveled Planner):** потокове K-Way злиття з розбиттям вихідного потоку на таблиці фіксованого розміру (`buildSplit`), стратегіями планування `SizeTieredCompactionPlanner` та `LeveledCompactionPlanner`, дедуплікацією за Sequence Number та безпечною утилізацією точкових і діапазонних Tombstones.
- **Підтримка API:** `put`, `get`, `delete`, `deleteRange`, `write(WriteBatch)`, `snapshot()` (`Snapshot` point-in-time MVCC ізоляція), `scan` (діапазонне читання) та `sync`.
- **Захист пам'яті:** динамічний backpressure під обмеженням купи (перевірено під `-Xmx32m` для потоку 500 МБ).
- **Стійкість до збоїв:** гарантоване відновлення стану після раптового завершення процесу (`kill -9`).

---

## 🛠️ Збірка та запуск тестів

Вимоги: **Java 21** та **Maven 3.8+**.

```bash
# Компіляція та збірка автономного JAR
./mvnw clean package -DskipTests

# Запуск повного набору тестів
./mvnw test
```

---

## 💻 Консольна демонстраційна утиліта (Віха M7)

Для демонстрації роботи рушія, інспекції бінарних 4 КБ блоків `.sst`, гарячого резервного копіювання, вимірювання швидкодії та перевірки відновлення після збоїв передбачено консольну утиліту `io.jlsm.cli.JLsmCli` (написану виключно на `java.base`):

```bash
# 1. Інтерактивний режим (REPL-консоль керування сховищем)
java -jar target/j-lsm-1.0.0-SNAPSHOT.jar --dir ./jlsm-data

# 2. Автоматична демонстрація життєвого циклу M7 (CRUD -> Flush 4KB SSTable -> Compaction -> Scan -> Torn-WAL Crash Recovery)
java -jar target/j-lsm-1.0.0-SNAPSHOT.jar --dir ./jlsm-demo demo

# 3. Бенчмарк швидкодії (Throughput ops/sec, Avg & P99 Latency для PUT, GET, SCAN, DELETE)
java -jar target/j-lsm-1.0.0-SNAPSHOT.jar --dir ./jlsm-bench --durability BUFFERED benchmark 5000 128

# 4. Діагностика цілісності MANIFEST, WAL, SSTable та розподілу дискового простору
java -jar target/j-lsm-1.0.0-SNAPSHOT.jar --dir ./jlsm-demo inspect

# 5. Побайтова інспекція структури SSTable (Header JSST, 4KB Data Blocks, Sparse Index, Footer JLSM)
java -jar target/j-lsm-1.0.0-SNAPSHOT.jar --dir ./jlsm-demo dump-sst
```

### Основні команди інтерактивної консолі (`j-lsm>`):
- `put <key> <value>` — запис або оновлення пари ключ-значення
- `get <key>` — точковий пошук ключа (`Active MemTable -> Immutable MemTable -> BloomFilter -> SparseIndex 4KB SSTables`)
- `delete <key>` (`del <key>`) — логічне видалення через запис `Tombstone`
- `delete-range <fromKey> <toKey>` (`del-range`) — інтервальне видалення діапазону `[fromKey, toKey)` через `RangeTombstone` (кадр WAL `0x04`)
- `batch <put:k=v | del:k | delrange:from..to> ...` — атомарний пакетний запис `WriteBatch` (кадр WAL `0x03`, принцип All-or-Nothing)
- `snap [fromKey|*] [toKey|*]` (`snapshot-scan`) — ізольоване point-in-time сканування діапазону через `Snapshot`
- `scan [fromKey|*] [toKey|*]` — діапазонне читання `[fromKey, toKey)` через `K-Way Merge`
- `metrics` (`stats`) — телеметрія в реальному часі (операції, заповнення RAM, кількість SSTable, лічильники Flush і Compaction)
- `inspect` (`diag`) — перевірка цілісності каталогу `MANIFEST`, `.wal` та `.sst` файлів
- `dump-sst <file.sst>` — дамп бінарної структури SSTable-файлу (Header, 4KB Data Blocks, Bloom Filter EstFPP, Sparse Index, Footer)
- `backup <dir>` / `verify-backup <dir>` / `restore <backupDir> <targetDir>` — гаряче резервне копіювання та відновлення з перевіркою CRC32
- `benchmark [count] [valueBytes]` — запуск навантажувального тесту швидкодії
- `demo` — повний сценарій демонстрації (CRUD, Flush, Compaction, WriteBatch, RangeDelete, Snapshot, Torn-WAL Recovery)
- `help` / `exit` — довідка та безпечне закриття бази
