# Установите зависимости
pip install -r requirements.txt

# Запустите MongoDB в Docker
docker run -d -p 27017:27017 --name mongodb -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=password mongo:6.0

# Запустите скрипт
python main.py --csv-dir ./data --mongo-uri "mongodb://admin:password@localhost:27017/"