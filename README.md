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

- **Append-only WAL** із захистом цілісності через CRC32 та відновленням після обривів запису (torn-write recovery).
- **Дворівневий MemTable:** активна таблиця для запису та незмінна (immutable) для фонового скидання (flush).
- **Формат SSTable:** вирівняні 4 КБ Data Blocks, бінарний Sparse Index та перевірка Magic Number у Footer.
- **Фоновий Compactor:** K-Way злиття із дедуплікацією за Sequence Number та безпечною утилізацією Tombstones.
- **Підтримка API:** `put`, `get`, `delete`, `scan` (діапазонне читання) та `sync`.
- **Захист пам'яті:** динамічний backpressure під обмеженням купи (перевірено під `-Xmx32m` для потоку 500 МБ).
- **Стійкість до збоїв:** гарантоване відновлення стану після раптового завершення процесу (`kill -9`).

---

## 🛠️ Збірка та запуск тестів

Вимоги: **Java 21** та **Maven 3.8+**.

```bash
# Компіляція проєкту
mvn clean compile

# Запуск повного набору тестів (176 тестів)
mvn test
