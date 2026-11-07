# Анализ архитектуры и предложения по улучшению: trade_app

| Поле | Значение |
|---|---|
| Область | репозиторий целиком (`fast_api` + `worker` + `common_libs` + `db`) |
| Метод | статический анализ исходного кода (только чтение) |
| Дата | 2026-10-09 |
| Основание | факты с якорями `path:line`; см. также `docs/README.md` |

---

## 1. Вердикт по архитектуре

**Что хорошо**

- Разделение на API и воркер через Redis Streams: API быстро отвечает, тяжёлая работа асинхронна (`docker-compose.yml:2-48`, `worker/src/worker.py:17-75`).
- Декларативные пайплайны задач в JSON — добавление нового типа данных без кода (`worker/src/jobs/task_settings.json:1-190`).
- Батчинг на фронт-этапе: до 10 сообщений в одной транзакции и pipeline-публикация (`worker/src/jobs/listeners.py:39-87`, `common_libs/redis_wrapper.py:63-67`).
- Идемпотентные UPSERT-insert (`ON CONFLICT DO UPDATE`) — `worker/src/jobs/sql/tasks/create_task.sql:4-8`, `common_libs/pg_wrapper.py:25-35`.
- Защита от перекрытия запусков слушателей: `max_instances: 1` (`topics_config.json:13,27`).
- Параллельные HTTP-запросы к MOEX через потоковый пул (`worker/src/jobs/_moex_call.py:16-22`).

**Системные проблемы**

1. **Поллинг вместо событий**: слушатели дёргают Redis по cron (1 с / 3 с) вместо блокирующего `XREAD BLOCK` — задержка до 3 с и пустые вызовы в простое (`worker/src/job_scheduler.py:41-59`, `topics_config.json:5-27`).
2. **Код не входит в образ**: `fast_api` и `worker` получают код через bind-монтирование с хоста (`docker-compose.yml:20-24,44-47,135-161`) — образы содержат только зависимости, сборка нерепродуцируема.
3. **Нет авторизации вообще**, включая деструктивные эндпоинты (`/api/pg/truncate/{table}`, `/api/redis/all` → `FLUSHALL`) — `app/backend/api/api_db/router.py:45-48`, `app/backend/api/api_redis/router.py:33-36`.
4. **Строковая сборка SQL** через `.format()`/интерполяцию — инъекции и хрупкость (`worker/src/jobs/listeners.py:143-157`, `worker/src/jobs/sql/tasks/update_task_status.sql:1-7`).
5. **Рассогласование схемы и кода**: Pydantic-модели в `api_tasks/schemas.py` декларативны — валидация query-параметров не выполняется (см. п. 2.1).
6. Тестов, CI, health-проверок, метрик, трейсинга в репозитории нет (не найдено).

---

## 2. Критические дефекты (P0)

| # | Дефект | Влияние | Якорь | Исправление |
|---|---|---|---|---|
| 1 | Pydantic-схемы не подключены к обработчику: `base_queue_endpoint` берёт `dict(request.query_params)`, а `user: Annotated[model, Depends()]` не используется | дефолты схемы не применяются; при опущенном `engine`/`market` в `moex_hist` — `KeyError` → задача сразу в `ERROR` | `app/backend/api/api_tasks/router.py:32-42`, `worker/src/jobs/jobs.py:208` | валидировать query через `Depends(model)` и использовать `model_dump()` как тело сообщения |
| 2 | Глобальный middleware возвращает `JSONResponse` без `status_code` → **HTTP 200 на любой ошибке** | мониторинг и клиенты не видят 5xx; фронтенд полагается на нестандартный заголовок `status: error` | `app/backend/main.py:36-39`, `app/frontend/static/js/index.js:41-56` | `JSONResponse(..., status_code=500)` + стандартное тело ошибки |
| 3 | API отвечает только `message_id` из Redis; **`task_id` (= `header.id`) клиенту не возвращается** | невозможно отследить статус задачи в `task_status` по ответу API | `app/backend/api/api_tasks/router.py:37-40` | возвращать `{'response': message_id, 'task_id': header['id']}` |
| 4 | `error_message` интерполируется в SQL и ограничен `VARCHAR(56)`; `repr(e)` легко длиннее, а кавычка в тексте ломает SQL | ошибка при UPDATE → исключение в слушателе → сообщение не ACK-нуто и застревает в PEL, задача навсегда в `PROGRESS` | `worker/src/jobs/listeners.py:143-157`, `worker/src/jobs/sql/tasks/update_task_status.sql:4`, `db/init_sql/init_tasks_db.sql:6` | параметризованный SQL + `TEXT` для ошибки |
| 5 | Потеря задачи при краше между `XACK front_topic` и `XADD tasks_topic` (порядок: сначала ACK, потом пересылка) | задача записана в БД как `NEW`, но в рабочую очередь не попала — «призрак» без восстановления | `worker/src/jobs/listeners.py:113-133` | сначала `bulk_publish` в `tasks_topic`, потом `XACK`; либо транзакционная outbox-логика |
| 6 | Нет восстановления pending-сообщений: `XREADGROUP ... '>'` читает только новые; после сбоя воркера сообщение остаётся в PEL навсегда | silent data loss; видны только через ручной `/api/redis/topic/get_uncommited_messages/all` | `common_libs/redis_wrapper.py:79-92` | периодический `XAUTOCLAIM`/`XPENDING` + re-delivery в слушателе |
| 7 | Мастер-задача переводится в `DONE` при первой завершившейся leaf-подзадаче (нет счётчика подзадач) | преждевременное `DONE`, пока остальные подзадачи ещё идут | `worker/src/jobs/listeners.py:198-207` | счётчик/флаг «все подзадачи завершены» (например, COUNT оставшихся `parent_task_id` в `task_status`) |
| 8 | `moex_hist` порождает **по одной подзадаче на день** с `start_trade_year=1997` → десятки тысяч задач/сообщений/строк БД за один запрос | взрыв fan-out, часы обработки, нагрузка на БД | `worker/src/jobs/jobs.py:216-220`, `.env:9` | батчить интервал (неделя/месяц) или использовать `sec_history_per_interval` (`common_libs/moex_api.py:60-75`) |
| 9 | Нет `timeout` и retry у HTTP к MOEX: `session.get(**call_params)` | один сбой/зависание MOEX → зависший поток или `ERROR` навсегда | `worker/src/jobs/_moex_call.py:50` | `timeout=(5,30)`, retry с backoff (urllib3 `Retry` / tenacity) |
| 10 | Новый экземпляр Chrome на каждый `secid` | старт браузера доминирует над скрейпингом; ~1 задача/сек максимум (n_bulk=1, cron */1) | `common_libs/selenium_wrapper.py:25-35`, `worker/src/jobs/finam_call.py:14-16`, `topics_config.json:17` | переиспользовать driver / пул браузеров; параллельные consumer'ы |
| 11 | `JsonFormatter` требует `record.__dict__['custom_fields']` → `KeyError` на любом `logger.info('x')` без `extra` | падение при логировании из любой точки, кроме LogMiddleware | `app/backend/logger/logger.py:19-20` | `record.__dict__.get('custom_fields', [])` |
| 12 | Конфиг читается в момент импорта: `int(getenv('start_trade_year'))` | отсутствие env → `TypeError` при старте приложения | `app/backend/api/api_tasks/schemas.py:39` | валидация настроек при старте (Pydantic Settings) с понятной ошибкой |
| 13 | `docker-compose.yml`: `TZ: ${timezone}'` (лишняя кавычка); мёртвый volume `postgresql_conf` с `device: ${postgresql_conf}$` (лишний `$`), не подключён ни к одному сервису | битое значение TZ в контейнере postgres; путь с `$` в device | `docker-compose.yml:70,163-168` | убрать кавычку/`$`, удалить мёртвый volume |
| 14 | `start.sh` логика pip-compile инверсна (`if [ -f requirements.in ]` → ничего не делать), а в образе `requirements.txt` удалён (`RUN rm requirements.txt`) | при запуске образа без bind-монтирования `pip-compile` падает (исходника нет); в compose «спасает» только bind-монтирование | `app/start.sh:2-6`, `app/Dockerfile:8-11`, `worker/start.sh:2-6` | компилировать lock на этапе сборки (`pip-compile` в Dockerfile), не в runtime |

**Требует проверки владельцем (не подтверждено кодом):** в `from_cursor` используется `hist_interval[1]` — второй элемент строки курсора MOEX как правая граница интервала (`worker/src/jobs/jobs.py:139-150`); если в ответе `history.cursor` второй столбец не является итоговым количеством, цикл либо не завершится, либо упадёт (`int <= str` → `TypeError`). Проверить по реальному ответу ISS.

---

## 3. Оптимизация производительности (P1)

| # | Что | Как | Якорь |
|---|---|---|---|
| 1 | Убрать cron-поллинг | `XREAD ... BLOCK 5000` в отдельном потоке или `BZPOPMIN`-подобную схему; либо увеличить `n_bulk` для `tasks_topic` и параллелить consumer'ов | `worker/src/job_scheduler.py:41-59`, `common_libs/redis_wrapper.py:79-92` |
| 2 | Пул соединений PostgreSQL | `psycopg2.pool.ThreadedConnectionPool` в `pg_wrapper`; сейчас новое соединение на каждую операцию (в цикле FINAM — на каждый `secid`) | `common_libs/pg_wrapper.py:11-16`, `worker/src/jobs/jobs.py:260-262` |
| 3 | Батчить обновления `finam_data` | `execute_many` вместо поочерёдного `execute` на каждый secid | `worker/src/jobs/jobs.py:260-262` |
| 4 | Пагинация MOEX: стоп при `len(data) < limit` | сейчас полнота определяется пустым ответом — последний «недогруженный» батч вызывает ещё `n_concurrent` запросов, почти все пустые | `worker/src/jobs/_moex_call.py:52-53`, `worker/src/jobs/jobs.py:107-118` |
| 5 | Переиспользовать Chrome-драйвер (пул) | один driver на worker или на задачу, а не на запрос | `common_libs/selenium_wrapper.py:25-35` |
| 6 | Индекс `task_status(status)` | поиск стейл-задач и статистика; сейчас индекс только по `parent_task_id` | `db/init_sql/init_tasks_db.sql:10` |
| 7 | Кэшировать чтение SQL-файлов | `get_sql_query` открывает файл на каждый запрос | `app/backend/api/api_db/router.py:31-35` |

---

## 4. Надёжность и эксплуатация (P1/P2)

| # | Что | Детали / якорь |
|---|---|---|
| 1 | Авторизация | хотя бы API-key middleware; allowlist таблиц для `/api/pg/truncate/{table_name}`; убрать `/api/redis/all` (FLUSHALL) или защитить — `app/backend/main.py:42-56`, `app/backend/api/api_db/router.py:45-48` |
| 2 | Health/readiness + `healthcheck` в compose + `restart: unless-stopped` для всех сервисов (сейчас только pgadmin имеет restart) — `docker-compose.yml:96` |
| 3 | Retry/backoff и таймауты для MOEX/FINAM/Redis/PG; сейчас одного сбоя достаточно для `ERROR` навсегда — `worker/src/jobs/_moex_call.py:50` |
| 4 | Обнаружение стейл-задач: `PROGRESS` дольше N минут → `ERROR`/повтор; столбец `retry_count` существует, но не заполняется — `db/init_sql/init_tasks_db.sql:6` |
| 5 | Graceful shutdown: обработка SIGTERM, ACK только после успешной записи; сейчас `while True: sleep(1)` и SIGKILL через 10 с — `worker/src/job_scheduler.py:58-59` |
| 6 | CORS: `allow_origins=['*']` + `allow_credentials=True` — браузеры блокируют такую комбинацию по специации Fetch — `app/backend/main.py:42-48` |
| 7 | Единый structured logging для обоих сервисов (JSON); сейчас fast_api → файл `/app/logs/fastapi_template.log`, worker → stdout в другом формате; `worker/src/logger/logger.py` — мёртвый код с путём `/app/logs` в воркере — `app/backend/logger/logger.py:29`, `worker/src/logger/legacy_logger.py:6` |
| 8 | Привилегии БД: `task_db_user`/`fin_db_user` создаются как `SUPERUSER` — достаточно `CREATEDB`/владельца БД — `db/init_sql/create_db.sql:1` |
| 9 | Jupyter: `privileged: true`, `user: root`, пустой токен — открыть только при необходимости, убрать privileged — `docker-compose.yml:103-130` |
| 10 | `read_secrets`: `split('=')` режет пароль, содержащий `=` → `split('=', 1)` — `common_libs/utils.py:6` |
| 11 | Корректные статусы/тела ошибок API и логирование: не писать полные тела сообщений в INFO (`logger.info(f'~~~{msg}')`) — `worker/src/jobs/listeners.py:187` |
| 12 | Секреты: `.env` с паролями лежит в рабочем каталоге (в `.gitignore` игнорируется, но присутствует); пример `fin_db_creds.txt` отсутствует в `secrets_example/` — `.env:20-35`, `secrets_example/` |

---

## 5. Гигиена кода и мёртвый код

- Неиспользуемые модули/классы: `app/backend/api/api_tasks/message_broker.py` (`MessageBroker`), `common_libs/redis_wrapper.py:108-163` (`redis_dict`), `common_libs/force_sync.py`, `worker/src/jobs/sql/fin_db/common_delete.sql`, `worker/src/job_scheduler.py:39` (`job_id_job_names`), `app/backend/api/api_db/router.py:28` (`pg_conn`), зависимость `openpyxl` (не импортируется) — `app/requirements.in:33`.
- Рассогласование схемы: `MessageBody`/`MessageHeader` с полем `task_name` не используются; реальный заголовок формируется с полем `type` — `app/backend/api/api_tasks/schemas.py:11-18` против `app/backend/api/api_tasks/router.py:37`.
- `RetryTask` (`task_retry`) имеет `"type": "tasks"`, для которого в `task_pipline` нет ветки → всегда `ERROR` — `worker/src/jobs/task_settings.json:34-36`, `worker/src/jobs/jobs.py:267-271`.
- Флаг `truncate` для MOEX-задач не работает: сравнение `bool == 'true'` — `worker/src/jobs/jobs.py:75` против `app/backend/api/api_tasks/schemas.py:27-28,33-34`.
- Рекомендации: единая версия Python для обоих образов (сейчас 3.9.16 EOL против 3.14.2), единая `redis-py` (7.0.1 против 6.2.0), `psycopg2-binary` в воркере вместо сборки из исходников (`worker/Dockerfile:8`), пин версий Chrome/Chromedriver в install-скриптах (`worker/install-chrome.sh:15`, `worker/install-chromedriver.sh:15`), тесты (pytest) и CI, `.format()`-SQL → параметризованные запросы, `common_libs` как нормальный пакет вместо `sys.path.append` + `importlib`-загрузки файлов (`common_libs/db_interface.py:10-38`).

---

## 6. Приоритетный план

**P0 (корректность, до эксплуатации):** пп. 1–8 из §2 — валидация query, status_code 500, возврат `task_id`, параметризованный SQL + `TEXT` для ошибок, порядок publish→ACK, `XAUTOCLAIM`, завершение мастер-задачи по всем подзадачам, таймауты HTTP.

**P1 (надёжность/производительность):** пп. 9–14 из §2 и §3 — retry/backoff, пул PG, блокирующее чтение очереди, батчинг FINAM, пул Chrome, healthcheck/restart, стейл-задачи.

**P2 (качество):** §4 пп. 1–12 и §5 — авторизация, единый логинг, чистка мёртвого кода, тесты, единые версии, воспроизводимые образы (код в образе, а не bind-монтирование).

**Ожидаемый эффект P0+P1:** устранение silent data loss (пп. 5–6 §2), сокращение времени обработки FINAM-выгрузки в десятки раз (пул браузеров + параллельные consumer'ы), устойчивость к сбоям MOEX/Redis без ручного вмешательства.
