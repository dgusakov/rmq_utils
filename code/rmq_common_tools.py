# -*- coding: utf-8 -*-

from datetime import datetime
import pika


def rabbit_connection_str(adr):
    """Возвращает строку подключения к "кролику" по заданному имени из словаря

    Args:
         adr: имя строки подключения из словаря

    Returns:
        Возвращает строку подключения к "кролику" в формате URLConnectionString
    """
    cp_rabbit = {
        # Задаем коннект к кролику по IP, %2F - означает vhost = "/"
        '192.168.1.25': 'amqp://rabbit_user:rabbit_password@192.168.1.25:5672/%2F',
        # Задаем коннект к кролику по HOSTNAME
        'my-rmq01': 'amqp://rabbit_user:rabbit_pass@my-rmq01:5672/my_vhost'
    }
    while adr not in cp_rabbit and adr != 'exit':
        adr = input(f'[{time_now()}] Указанный "кролик" не найден в справочнике! \n'
                    'Введите корректный адрес сервера RabbitMQ или "exit" для прекращения работы \n')
    if adr == 'exit':
        exit()
    return cp_rabbit[adr]

def time_now():
    return datetime.now().strftime('%H:%M:%S')


def console_log(*args):
    print(f'[{time_now()}] {" ".join(args)}')


def rmq_connect(rabbit_address):
    # устанавливаем соединение с сервером RabbitMQ
    parameters = pika.URLParameters(rabbit_connection_str(rabbit_address))
    console_log("Работаем на кролике:", rabbit_address)
    connection = pika.BlockingConnection(parameters)
    console_log("Подключение успешно")
    return connection


def rmq_disconnect(connection):
    connection.close()
