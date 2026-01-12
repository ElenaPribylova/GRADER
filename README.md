# ETL Скрипт для Grader API

Автоматическая загрузка данных о попытках студентов из API грейдера в локальную базу PostgreSQL с логированием, статистикой в Google Sheets и email-оповещениями.

## Возможности

- Получение данных из API онлайн-университета
- Обработка и валидация данных (включая парсинг passback_params)
- Сохранение в PostgreSQL
- Логирование с автоматической очисткой старых логов (хранение 3 дня)
- Агрегация дневной статистики
- Загрузка статистики в Google Sheets
- Email-оповещения о завершении работы

## Требования

- Python 3.11+
- PostgreSQL 12+
- Gmail аккаунт (для email)
- Google Cloud проект (для Sheets, опционально)

## Установка

### 1. Клонирование и настройка окружения
```bash
mkdir grader_etl
cd grader_etl

python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

pip install -r requirements.txt
```

### 2. Настройка PostgreSQL
```bash
# Создать базу данных
psql -U postgres
CREATE DATABASE grader_db;
\q
```

### 3. Настройка Gmail

Для работы email-оповещений:

1. Включить 2FA в Google аккаунте
2. Создать пароль приложения: https://myaccount.google.com/apppasswords
3. Использовать этот пароль в .env

### 4. Настройка Google Sheets (опционально)

1. Создать проект в https://console.cloud.google.com/
2. Включить Google Sheets API и Google Drive API
3. Создать Service Account
4. Скачать JSON ключ как credentials.json
5. Поделиться таблицей с email из credentials.json

### 5. Создать .env файл

Скопировать содержимое из примера выше или использовать:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=grader_db
DB_USER=postgres
DB_PASSWORD=12345

EMAIL_ENABLED=true
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SENDER_EMAIL=your_email@gmail.com
SENDER_PASSWORD=your_app_password
RECIPIENT_EMAIL=recipient@gmail.com
```

## Запуск
```bash
python etl_script.py
```

## Структура базы данных
```sql
CREATE TABLE grader_attempts (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    oauth_consumer_key VARCHAR(255),
    lis_result_sourcedid TEXT,
    lis_outcome_service_url TEXT,
    is_correct BOOLEAN,
    attempt_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Логирование

Логи сохраняются в папку `logs/` с именем `etl_YYYY-MM-DD.log`. Автоматически удаляются логи старше 3 дней.

## Автоматизация

### Windows (Task Scheduler)

1. Открыть Task Scheduler
2. Create Basic Task
3. Trigger: Daily, время 02:00
4. Action: Start a program
   - Program: `C:\path\to\venv\Scripts\python.exe`
   - Arguments: `etl_script.py`
   - Start in: `C:\path\to\grader_etl`

### Linux (cron)
```bash
crontab -e

# Добавить строку (запуск каждый день в 02:00)
0 2 * * * cd /path/to/grader_etl && /path/to/venv/bin/python etl_script.py
```

## Проверка работы
```bash
# Просмотр логов
cat logs/etl_2024-01-11.log

# Проверка данных в БД
psql -U postgres -d grader_db -c "SELECT COUNT(*) FROM grader_attempts;"

# Последние записи
psql -U postgres -d grader_db -c "SELECT * FROM grader_attempts ORDER BY created_at DESC LIMIT 5;"
```

## Статистика

Скрипт собирает следующую статистику за день:

- Всего попыток
- Успешных попыток
- Неуспешных попыток
- Run попыток
- Уникальных пользователей
- Success Rate %

## Troubleshooting

### Ошибка подключения к PostgreSQL
```bash
# Проверить статус сервиса
# Windows
net start postgresql-x64-12

# Linux
sudo systemctl status postgresql
```

### Ошибка Gmail authentication

- Проверить, что включена 2FA
- Использовать пароль приложения, не обычный пароль
- Проверить, что SMTP_PORT=587 для STARTTLS

### Ошибка Google Sheets

- Убедиться, что включены оба API (Sheets и Drive)
- Проверить, что credentials.json в корне проекта
- Поделиться таблицей с service account email

## Лицензия

MIT