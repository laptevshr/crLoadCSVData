import os
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, BulkWriteError
import logging
from typing import List, Optional
from datetime import datetime
import argparse

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CSVToMongoDBLoader:
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str):
        """
        Инициализация загрузчика
        
        Args:
            mongo_uri: URI подключения к MongoDB
            db_name: Название базы данных
            collection_name: Название коллекции
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        
    def connect_to_mongodb(self) -> bool:
        """Подключение к MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            # Проверка подключения
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info(f"Успешное подключение к MongoDB: {self.mongo_uri}")
            return True
        except ConnectionFailure as e:
            logger.error(f"Ошибка подключения к MongoDB: {e}")
            return False
    
    def disconnect(self):
        """Отключение от MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Отключение от MongoDB")
    
    def read_csv_files(self, csv_directory: str, file_pattern: str = "*.csv") -> List[pd.DataFrame]:
        """
        Чтение CSV файлов из директории
        
        Args:
            csv_directory: Путь к директории с CSV файлами
            file_pattern: Паттерн для поиска файлов
            
        Returns:
            Список DataFrame с данными
        """
        csv_files = []
        dataframes = []
        
        try:
            # Поиск всех CSV файлов в директории
            for file in os.listdir(csv_directory):
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(csv_directory, file))
            
            if not csv_files:
                logger.warning(f"CSV файлы не найдены в директории: {csv_directory}")
                return dataframes
            
            logger.info(f"Найдено {len(csv_files)} CSV файлов")
            
            # Чтение каждого CSV файла
            for csv_file in csv_files:
                try:
                    # Чтение CSV с указанием заголовков и преобразованием дат
                    df = pd.read_csv(
                        csv_file,
                        parse_dates=['Open time', 'Close time'],
                        infer_datetime_format=True
                    )
                    
                    # Добавляем поле с названием файла
                    df['source_file'] = os.path.basename(csv_file)
                    df['import_timestamp'] = datetime.now()
                    
                    dataframes.append(df)
                    logger.info(f"Прочитан файл: {os.path.basename(csv_file)} - {len(df)} строк")
                    
                except Exception as e:
                    logger.error(f"Ошибка чтения файла {csv_file}: {e}")
                    
        except Exception as e:
            logger.error(f"Ошибка при чтении CSV файлов: {e}")
        
        return dataframes
    
    def prepare_data_for_mongodb(self, df: pd.DataFrame) -> List[dict]:
        """
        Подготовка данных для вставки в MongoDB
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Список словарей для вставки в MongoDB
        """
        # Преобразование DataFrame в список словарей
        records = df.to_dict('records')
        
        # Преобразование типов данных и переименование полей
        for record in records:
            # Преобразование числовых полей
            numeric_fields = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote asset volume', 
                             'Number of trades', 'Taker buy base asset volume', 
                             'Taker buy quote asset volume', 'Ignore']
            
            for field in numeric_fields:
                if field in record and pd.notna(record[field]):
                    try:
                        record[field] = float(record[field])
                    except (ValueError, TypeError):
                        record[field] = None
            
            # Преобразование поля Number of trades в integer, если возможно
            if 'Number of trades' in record and pd.notna(record['Number of trades']):
                try:
                    record['Number of trades'] = int(record['Number of trades'])
                except (ValueError, TypeError):
                    record['Number of trades'] = None
        
        return records
    
    def create_indexes(self):
        """Создание индексов для оптимизации запросов"""
        try:
            # Создание индекса по временным меткам
            index_info = []
            
            if 'Open time' in next(self.collection.find().limit(1), {}):
                self.collection.create_index([("Open time", 1)])
                index_info.append("Open time")
            
            if 'Close time' in next(self.collection.find().limit(1), {}):
                self.collection.create_index([("Close time", 1)])
                index_info.append("Close time")
            
            # Индекс по source_file для фильтрации по файлам
            if 'source_file' in next(self.collection.find().limit(1), {}):
                self.collection.create_index([("source_file", 1)])
                index_info.append("source_file")
            
            if index_info:
                logger.info(f"Созданы индексы по полям: {', '.join(index_info)}")
            else:
                logger.info("Индексы не созданы (поля не найдены)")
            
        except Exception as e:
            logger.warning(f"Ошибка при создании индексов: {e}")
    
    def load_data_to_mongodb(self, csv_directory: str, batch_size: int = 1000) -> bool:
        """
        Загрузка данных из CSV файлов в MongoDB
        
        Args:
            csv_directory: Путь к директории с CSV файлами
            batch_size: Размер батча для вставки
            
        Returns:
            True если успешно, False если ошибка
        """
        if not self.connect_to_mongodb():
            return False
        
        try:
            # Чтение CSV файлов
            dataframes = self.read_csv_files(csv_directory)
            
            if not dataframes:
                logger.error("Нет данных для загрузки")
                return False
            
            total_records = 0
            
            # Обработка каждого DataFrame
            for i, df in enumerate(dataframes):
                logger.info(f"Обработка датафрейма {i+1}/{len(dataframes)}")
                
                # Подготовка данных
                records = self.prepare_data_for_mongodb(df)
                
                # Вставка данных батчами
                for j in range(0, len(records), batch_size):
                    batch = records[j:j + batch_size]
                    try:
                        result = self.collection.insert_many(batch, ordered=False)
                        total_records += len(result.inserted_ids)
                        logger.info(f"Вставлено {len(result.inserted_ids)} записей (батч {j//batch_size + 1})")
                    except BulkWriteError as e:
                        # Логирование ошибок, но продолжение обработки
                        error_count = len(e.details['writeErrors'])
                        logger.warning(f"Ошибки при вставке батча: {error_count} ошибок")
                        # Подсчет успешных вставок
                        successful_inserts = len(batch) - error_count
                        total_records += successful_inserts
            
            logger.info(f"Всего загружено записей: {total_records}")
            
            # Создание индексов после загрузки данных
            self.create_indexes()
            
            return True
            
        except Exception as e:
            logger.error(f"Критическая ошибка при загрузке данных: {e}")
            return False
        finally:
            self.disconnect()

def main():
    parser = argparse.ArgumentParser(description='Загрузка OHLCVT данных из CSV в MongoDB')
    parser.add_argument('--csv-dir', type=str, required=True, 
                       help='Путь к директории с CSV файлами')
    parser.add_argument('--mongo-uri', type=str, 
                       default='mongodb://localhost:27017/',
                       help='URI подключения к MongoDB (по умолчанию: mongodb://localhost:27017/)')
    parser.add_argument('--db-name', type=str, default='financial_data',
                       help='Название базы данных (по умолчанию: financial_data)')
    parser.add_argument('--collection', type=str, default='ohlcvt_data',
                       help='Название коллекции (по умолчанию: ohlcvt_data)')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Размер батча для вставки (по умолчанию: 1000)')
    
    args = parser.parse_args()
    
    # Создание загрузчика
    loader = CSVToMongoDBLoader(
        mongo_uri=args.mongo_uri,
        db_name=args.db_name,
        collection_name=args.collection
    )
    
    # Загрузка данных
    success = loader.load_data_to_mongodb(
        csv_directory=args.csv_dir,
        batch_size=args.batch_size
    )
    
    if success:
        logger.info("Загрузка данных завершена успешно!")
    else:
        logger.error("Загрузка данных завершена с ошибками!")

if __name__ == "__main__":
    main()