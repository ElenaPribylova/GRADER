import os
import sys
import json
import logging
import ast
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

try:
    import gspread
    from google.oauth2.service_account import Credentials

    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

load_dotenv()

API_URL = "https://b2b.itresume.ru/api/statistics"
API_CLIENT = "Skillfactory"
API_CLIENT_KEY = "M2MGWS"

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'grader_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '12345'),
    'client_encoding': 'utf8'
}

GSHEETS_CREDENTIALS = os.getenv('GSHEETS_CREDENTIALS_FILE', 'credentials.json')
GSHEETS_SPREADSHEET = os.getenv('GSHEETS_SPREADSHEET_NAME', 'Grader Statistics')
GSHEETS_ENABLED = os.getenv('GSHEETS_ENABLED', 'false').lower() == 'true'

START_DATE = os.getenv('START_DATE', '2023-05-31 00:00:00.000000')
END_DATE = os.getenv('END_DATE', '2023-05-31 23:59:59.999999')

LOGS_DIR = Path('logs')
LOGS_RETENTION_DAYS = 3


def setup_logging():
    LOGS_DIR.mkdir(exist_ok=True)
    cleanup_old_logs()

    log_file = LOGS_DIR / f"etl_{datetime.now().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Логирование инициализировано: {log_file}")
    return logger


def cleanup_old_logs():
    if not LOGS_DIR.exists():
        return

    cutoff_date = datetime.now() - timedelta(days=LOGS_RETENTION_DAYS)

    for log_file in LOGS_DIR.glob("etl_*.log"):
        try:
            file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_time < cutoff_date:
                log_file.unlink()
        except Exception as e:
            print(f"Ошибка удаления лога {log_file}: {e}")


def fetch_data_from_api(start_date, end_date, logger):
    params = {
        'client': API_CLIENT,
        'client_key': API_CLIENT_KEY,
        'start': start_date,
        'end': end_date
    }

    try:
        logger.info(f"Запрос к API: {start_date} - {end_date}")
        response = requests.get(API_URL, params=params, timeout=60)

        if response.status_code == 200:
            data = response.json()
            logger.info(f"Получено записей: {len(data)}")
            return data
        else:
            logger.error(f"Ошибка API: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка JSON: {e}")
        return None


def parse_passback_params(passback_str, logger):
    try:
        if passback_str is None or passback_str == 'None':
            return {}

        passback_dict = ast.literal_eval(passback_str)
        if isinstance(passback_dict, dict):
            return passback_dict
        logger.warning(f"passback_params не словарь: {type(passback_dict)}")
        return {}
    except (ValueError, SyntaxError) as e:
        logger.warning(f"Ошибка парсинга passback_params: {e}")
        return {}


def validate_record(record, logger):
    try:
        required = ['lti_user_id', 'passback_params', 'attempt_type', 'created_at']

        for field in required:
            if field not in record:
                logger.warning(f"Отсутствует поле: {field}")
                return None

        passback_dict = parse_passback_params(record['passback_params'], logger)

        attempt_type = record['attempt_type']
        if attempt_type not in ['run', 'submit']:
            logger.warning(f"Неверный attempt_type: {attempt_type}")
            return None

        is_correct = record.get('is_correct')
        if is_correct is not None:
            if isinstance(is_correct, int):
                is_correct = bool(is_correct)
            elif not isinstance(is_correct, bool):
                logger.warning(f"Неверный is_correct: {type(is_correct)}")
                return None

        try:
            datetime.strptime(record['created_at'], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            try:
                datetime.strptime(record['created_at'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                logger.warning(f"Неверный формат даты: {record['created_at']}")
                return None

        return {
            'user_id': str(record['lti_user_id']),
            'oauth_consumer_key': str(passback_dict.get('oauth_consumer_key', '')),
            'lis_result_sourcedid': str(passback_dict.get('lis_result_sourcedid', '')),
            'lis_outcome_service_url': str(passback_dict.get('lis_outcome_service_url', '')),
            'is_correct': is_correct,
            'attempt_type': attempt_type,
            'created_at': record['created_at']
        }

    except Exception as e:
        logger.error(f"Ошибка валидации: {e}")
        return None


def process_data(raw_data, logger):
    logger.info("Начало обработки данных")

    validated = []
    skipped = 0

    for record in raw_data:
        valid = validate_record(record, logger)
        if valid:
            validated.append(valid)
        else:
            skipped += 1

    logger.info(f"Обработано: {len(validated)}, пропущено: {skipped}")
    return validated


def create_database_table(logger):
    query = """
            CREATE TABLE IF NOT EXISTS grader_attempts \
            ( \
                id \
                SERIAL \
                PRIMARY \
                KEY, \
                user_id \
                VARCHAR \
            ( \
                255 \
            ) NOT NULL,
                oauth_consumer_key VARCHAR \
            ( \
                255 \
            ),
                lis_result_sourcedid TEXT,
                lis_outcome_service_url TEXT,
                is_correct BOOLEAN,
                attempt_type VARCHAR \
            ( \
                50 \
            ) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

            CREATE INDEX IF NOT EXISTS idx_user_id ON grader_attempts(user_id);
            CREATE INDEX IF NOT EXISTS idx_created_at ON grader_attempts(created_at);
            CREATE INDEX IF NOT EXISTS idx_attempt_type ON grader_attempts(attempt_type); \
            """

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
                logger.info("Таблица создана/проверена")
    except psycopg2.Error as e:
        logger.error(f"Ошибка создания таблицы: {e}")
        raise


def load_data_to_database(records, logger):
    if not records:
        logger.warning("Нет записей для загрузки")
        return 0

    logger.info(f"Загрузка {len(records)} записей в БД")

    query = """
            INSERT INTO grader_attempts
            (user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url,
             is_correct, attempt_type, created_at)
            VALUES %s \
            """

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                values = [
                    (r['user_id'], r['oauth_consumer_key'], r['lis_result_sourcedid'],
                     r['lis_outcome_service_url'], r['is_correct'], r['attempt_type'],
                     r['created_at'])
                    for r in records
                ]

                execute_values(cursor, query, values)
                conn.commit()

                count = cursor.rowcount
                logger.info(f"Загружено записей: {count}")
                return count

    except psycopg2.Error as e:
        logger.error(f"Ошибка загрузки в БД: {e}")
        raise


def get_daily_statistics(date, logger):
    query = """
            SELECT COUNT(*) as total, \
                   COUNT(*)    FILTER (WHERE is_correct = true) as success, COUNT(*) FILTER (WHERE is_correct = false) as failed, COUNT(*) FILTER (WHERE is_correct IS NULL) as runs, COUNT(DISTINCT user_id) as users, \
                   COUNT(*)    FILTER (WHERE attempt_type = 'run') as run_cnt, COUNT(*) FILTER (WHERE attempt_type = 'submit') as submit_cnt
            FROM grader_attempts
            WHERE DATE (created_at) = %s \
            """

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (date,))
                result = cursor.fetchone()

                if result:
                    stats = {
                        'date': date,
                        'total_attempts': result[0],
                        'successful_attempts': result[1],
                        'failed_attempts': result[2],
                        'run_attempts': result[3],
                        'unique_users': result[4],
                        'run_count': result[5],
                        'submit_count': result[6],
                        'success_rate': round(
                            (result[1] / result[6] * 100) if result[6] > 0 else 0, 2
                        )
                    }
                    logger.info(f"Статистика за {date}: {stats}")
                    return stats

                return None

    except psycopg2.Error as e:
        logger.error(f"Ошибка получения статистики: {e}")
        return None


def upload_to_google_sheets(stats, logger):
    if not GSHEETS_ENABLED:
        logger.info("Google Sheets отключен")
        return False

    if not GSHEETS_AVAILABLE:
        logger.warning("gspread не установлен")
        return False

    if not Path(GSHEETS_CREDENTIALS).exists():
        logger.warning(f"Файл credentials не найден: {GSHEETS_CREDENTIALS}")
        return False

    try:
        logger.info("Загрузка в Google Sheets")

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        creds = Credentials.from_service_account_file(GSHEETS_CREDENTIALS, scopes=scopes)
        client = gspread.authorize(creds)

        try:
            spreadsheet = client.open(GSHEETS_SPREADSHEET)
        except gspread.SpreadsheetNotFound:
            spreadsheet = client.create(GSHEETS_SPREADSHEET)
            logger.info(f"Создана таблица: {GSHEETS_SPREADSHEET}")

        try:
            worksheet = spreadsheet.worksheet("Daily Statistics")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet("Daily Statistics", 1000, 10)
            headers = ['Дата', 'Всего попыток', 'Успешных', 'Неуспешных',
                       'Run попыток', 'Уникальных пользователей',
                       'Run count', 'Submit count', 'Success Rate %']
            worksheet.append_row(headers)

        row_data = [
            stats['date'], stats['total_attempts'], stats['successful_attempts'],
            stats['failed_attempts'], stats['run_attempts'], stats['unique_users'],
            stats['run_count'], stats['submit_count'], stats['success_rate']
        ]

        worksheet.append_row(row_data)
        logger.info("Данные загружены в Google Sheets")
        return True

    except Exception as e:
        logger.error(f"Ошибка Google Sheets: {e}")
        return False


def main():
    logger = setup_logging()
    logger.info(f"Старт ETL: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        start_str = START_DATE
        end_str = END_DATE

        logger.info(f"Период: {start_str} - {end_str}")

        raw_data = fetch_data_from_api(start_str, end_str, logger)

        if raw_data is None:
            logger.error("Не удалось получить данные из API")
            return

        if len(raw_data) == 0:
            logger.info("API вернул 0 записей")
            return

        validated_data = process_data(raw_data, logger)

        if len(validated_data) == 0:
            logger.warning("После валидации нет записей")
            return

        create_database_table(logger)
        loaded_count = load_data_to_database(validated_data, logger)

        logger.info(f"ETL завершен. Загружено: {loaded_count}")

        date_for_stats = start_str.split()[0]
        stats = get_daily_statistics(date_for_stats, logger)

        if stats:
            upload_to_google_sheets(stats, logger)

        logger.info("Завершение: Успешно")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        logger.info("Завершение: С ошибками")

    finally:
        logger.info(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()