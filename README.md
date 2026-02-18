# CSV Reconciler (сверка транзакций из CSV)

Небольшая утилита для сверки **двух больших CSV** с транзакциями по:

- `transaction_id`
- сумме (`amount`)
- статусу (`status`)

Особенности:

- **Без внешних зависимостей**: только Python + стандартная библиотека (используется `sqlite3`)
- Работает с большими файлами: импортирует CSV в локальную SQLite БД на диск и сравнивает SQL-запросами
- Генерирует отчёты: отсутствующие транзакции, несовпадения суммы/статуса, дубликаты `transaction_id`, агрегаты по статусам

## Требования

- Python 3.9+ (подойдёт любой установленный Python, например из Homebrew или python.org)

Если при запуске `python3` вы видите сообщение про `xcode-select`, значит Python не установлен как отдельный пакет.
Самый простой способ на macOS:

```bash
brew install python
```

## Быстрый старт

```bash
python3 csv_reconcile.py \
  --left /path/to/left.csv \
  --right /path/to/right.csv \
  --out ./out \
  --id-col transaction_id \
  --amount-col amount \
  --status-col status
```

После выполнения появится папка `./out` с отчётами.

## Режим с конфигом (разные колонки в разных файлах, несколько CSV)

Если в разных CSV нужные поля находятся в разных столбцах (или называются по‑разному), используйте JSON‑конфиг и параметр `--config`.

Пример `reconcile.json`:

```json
{
  "out_dir": "./out",
  "amount_scale": 2,
  "amount_tolerance": 0,
  "primary": "bank",
  "files": [
    {
      "name": "bank",
      "path": "/path/to/bank.csv",
      "delimiter": ";",
      "encoding": "utf-8",
      "decimal_comma": true,
      "index_base": 0,
      "columns": {
        "id": { "name": "transaction_id" },
        "amount": { "index": 5 },
        "status": { "name": "status" }
      },
      "keep_cols": ["created_at", "merchant"]
    },
    {
      "name": "crm",
      "path": "/path/to/crm.csv",
      "delimiter": ",",
      "encoding": "utf-8",
      "decimal_comma": false,
      "columns": {
        "id": { "name": "txn" },
        "amount": { "name": "sum" },
        "status": { "index": 7 }
      }
    }
  ]
}
```

Запуск:

```bash
python3 csv_reconcile.py --config /path/to/reconcile.json
```

Что важно про колонки:

- Можно задавать по **имени**: `{ "name": "transaction_id" }`
- Можно задавать по **индексу**: `{ "index": 5 }`
- По умолчанию `index` считается **0-based** (первый столбец = 0). Если удобнее 1-based, задайте `"index_base": 1` на уровне файла или корня конфига.

Результат:

- В `out_dir/summary.json` будет сводка
- Для каждой пары `primary__vs__<other>` создаётся подпапка с:
  - `missing_in_base.csv`
  - `missing_in_other.csv`
  - `mismatches.csv`
  - `duplicates_base.csv`
  - `duplicates_other.csv`

Также в корне `out_dir` создаются агрегаты по статусам: `status_totals__<name>.csv`.

## Важные параметры

- `--delimiter` — разделитель (по умолчанию `,`)
- `--encoding` — кодировка (по умолчанию `utf-8`)
- `--amount-scale` — сколько знаков после запятой считать “копейками” (по умолчанию `2`)
- `--amount-tolerance` — допустимая разница в **минимальных единицах** (например, при `--amount-scale 2` значение `1` = 0.01)
- `--decimal-comma` — принудительно трактовать запятую как десятичный разделитель (полезно для `12,34`)
- `--keep-cols` — дополнительные колонки, которые нужно сохранить и вывести в отчёты (через запятую)

## Что создаётся в `--out`

- `summary.json` — сводка по результатам
- `missing_in_left.csv` — есть в правом файле, нет в левом
- `missing_in_right.csv` — есть в левом файле, нет в правом
- `mismatches.csv` — совпал `transaction_id`, но отличаются сумма и/или статус
- `duplicates_left.csv` — дубликаты `transaction_id` в левом файле
- `duplicates_right.csv` — дубликаты `transaction_id` в правом файле
- `status_totals_left.csv` / `status_totals_right.csv` — агрегаты по статусам (count, sum)
- `reconcile.sqlite` — промежуточная SQLite БД (можно удалять после анализа)

В режиме `--config` структура отчётов немного другая (см. выше).

## Примечания по точности сумм

Чтобы избежать ошибок float:

- сумма парсится в `Decimal`
- затем переводится в **целое число** `amount_scaled = amount * 10^amount_scale`
- сравнение сумм идёт по `amount_scaled` с учётом `--amount-tolerance`

## Web-версия (GitHub Pages)

В папке `docs/` есть статическая web-версия, которая делает сверку **в браузере** (файлы не загружаются на сервер).

### Локальный запуск

Самый простой способ — поднять статический сервер в корне репозитория и открыть `docs/`:

```bash
cd csv-reconciler
npx serve .
```

Затем откройте адрес, который покажет `serve`, и перейдите в `/docs/`.

### Публикация на GitHub Pages

1) Запушьте репозиторий на GitHub  
2) В настройках репозитория: **Settings → Pages**  
3) В **Build and deployment** выберите:

- **Source**: Deploy from a branch
- **Branch**: `main` (или ваша ветка)
- **Folder**: `/docs`

После этого страница будет доступна по адресу вида:
`https://<username>.github.io/<repo>/`

