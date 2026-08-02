import pika
import numpy as np
import pandas as pd
import json
from datetime import datetime
import time

# Создаем бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Загружаем датасет о диабете
        path = input('Укажите путь к файлу с данными в формате csv')
        X = pd.read_csv('path')
        print('Загруженные данные')
        print('-----------------------------------------------------------------')
        print(X)

        for id in X['id']:
            # Создаем подключение по адресу rabbitmq:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()

            '''# Создадим очередь y_true
            channel.queue_declare(queue='y_true')'''
            # Создадим очередь features
            channel.queue_declare(queue='features')

            # Создаем идентификатор сообщения
            message_id = ('ID полиса:', id, datetime.timestamp(datetime.now().strftime()))

            # Опубликуем сообщение в очередь y_true
            '''message_y_true = {
                'id': message_id,
                'body': y[random_row]
            }
            channel.basic_publish(exchange='',
                                routing_key='y_true',
                                body=json.dumps(message_y_true))
            print('Сообщение с правильным ответом отправлено в очередь')'''

            # Опубликуем сообщение в очередь features
            message_features = {
                'id': message_id,
                'body': X[X['id'] == id]
            }
            channel.basic_publish(exchange='',
                                routing_key='features',
                                body=json.dumps(message_features))
            print('Сообщение с вектором признаков отправлено в очередь')

            # Закроем подключение 
            connection.close()
            
            # Делаем задержку на 1 секунду 
            time.sleep(1)        

    except ValueError:
        print('Ошибка в данных')
        exit(0)
    except TypeError:
        print('Ошибка типа данных')
        exit(0)
    except Exception as error:
        print('Не удалось подключиться к очереди: {}'.format(error))
        exit(0)
