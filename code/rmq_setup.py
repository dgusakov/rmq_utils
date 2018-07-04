#подключаем необходимые библиотеки

import pika                             		# библиотека для работы с AMQP протоколом
import rmq_connection_parameters as rmq_params 	# конфигурационный файл с параметрами конекта к кроликам
from datetime import datetime           		# библиотека для вывода времени в консоль
import sys
import argparse                         		# парсер аргументов командной строки
import traceback                        		# модуль для вывода трейса ошибки

version = "0.2"

### 0.1 - Первая версия. Документировать лень.
### 0.2 - Добавлены обработчикм исключений при считывании сообщений. Исправлена ошибка создания Exchange

def createParser ():
    # Создаем класс парсера
    parser = argparse.ArgumentParser(prog = 'RMQ_SETUP_UTILS',
                                     description = '''Набор базовых операций для работы с RabbitMQ по протоколу AMQP''',
                                     epilog = '''(c) Dmitry.V.Gusakov 2018.
                                     Автор программы, как всегда, не несет никакой ответственности ни за что.''',
                                     add_help = False
                                     )
    # Создаем группу параметров для родительского парсера,
    # ведь у него тоже должен быть параметр --help / -h
    parent_group = parser.add_argument_group (title='Параметры')
    parent_group.add_argument ('--help', '-h', action='help', help='Справка')
    parent_group.add_argument ('--version',
                               action='version',
                               help = 'Вывести номер версии',
                               version='%(prog)s {}'.format (version))

    # Создаем группу подпарсеров
    subparsers = parser.add_subparsers (dest = 'command',
                                        title = 'Возможные команды',
                                        description = 'Команды, которые должны быть в качестве первого параметра %(prog)s')

    # Создаем парсер для команды create_que
    create_que_parser = subparsers.add_parser ('create_que',
                                               #add_help = False,
                                               help = 'Режим создания очереди',
                                               description = '''Запуск в режиме создания очереди''')
    # Создаем новую группу параметров
    create_que_group = create_que_parser.add_argument_group (title='Параметры')

    create_que_group.add_argument ('-rmq', '--rabbit_address', required=True,
                                   help = 'Hostname или IP кролика',
                                   metavar = 'Rabbit_address')
    create_que_group.add_argument ('-q', '--queue', required=True,
                                   help = 'Имя очереди')
    create_que_group.add_argument ('-d', '--durable', action='store_true', default=False,
                                   help = 'Признак DURABLE для очереди. По умолчанию False')

    # Создаем парсер для команды create_exch
    create_exch_parser = subparsers.add_parser ('create_exch',
                                                #add_help = False,
                                                help = 'Режим создания exchange',
                                                description = '''Запуск в режиме создания exchange''')
    # Создаем новую группу параметров
    create_exch_group = create_exch_parser.add_argument_group (title='Параметры')

    create_exch_group.add_argument ('-rmq', '--rabbit_address', required=True,
                                    help = 'Hostname или IP кролика',
                                    metavar = 'Rabbit_address')
    create_exch_group.add_argument ('-e', '--exch', required=True,
                                    help = 'Имя exchange',
                                    metavar = 'Exchange')
    create_exch_group.add_argument ('-t', '--type', choices=['direct', 'topic', 'fanout','headers'], default='topic',
                                    help = 'Тип создаваемого exchange. По умолчанию Topic')
    create_exch_group.add_argument ('-d', '--durable', action='store_true', default=False,
                                    help = 'Признак DURABLE для exchange. По умолчанию False')

    # Создаем парсер для команды delete
    delete_parser = subparsers.add_parser ('delete',
                                           #add_help = False,
                                           help = 'Режим удаления очереди или exchange',
                                           description = '''Запуск в режиме удаления очереди или exchange''')
    # Создаем новую группу параметров
    delete_group = delete_parser.add_argument_group (title='Параметры')

    delete_group.add_argument ('-rmq', '--rabbit_address', required=True,
                               help = 'Hostname или IP кролика',
                               metavar = 'Rabbit_address')
    delete_group.add_argument ('-q', '--queue',
                               help = 'Имя очереди')
    delete_group.add_argument ('-e', '--exch',
                               help = 'Имя exchange',
                               metavar = 'Exchange')

    # Создаем парсер для команды bind
    bind_parser = subparsers.add_parser ('bind',
                                         #add_help = False,
                                         help = 'Режим создания binding',
                                         description = '''Запуск в режиме создания binding''')
    # Создаем новую группу параметров
    bind_group = bind_parser.add_argument_group (title='Параметры')

    bind_group.add_argument ('-rmq', '--rabbit_address', required=True,
                             help = 'Hostname или IP кролика',
                             metavar = 'Rabbit_address')
    bind_group.add_argument ('-q', '--queue', required=True,
                             help = 'Имя очереди')
    bind_group.add_argument ('-e', '--exch', required=True,
                             help = 'Имя exchange',
                             metavar = 'Exchange')
    bind_group.add_argument ('-rk', '--r_key', required=True,
                             help = 'Routing_key для биндинга',
                             metavar = 'Routing_key')

    # Создаем парсер для команды unbind
    unbind_parser = subparsers.add_parser ('unbind',
                                           #add_help = False,
                                           help = 'Режим удаления binding',
                                           description = '''Запуск в режиме удаления binding''')
    # Создаем новую группу параметров
    unbind_group = unbind_parser.add_argument_group (title='Параметры')

    unbind_group.add_argument ('-rmq', '--rabbit_address', required=True,
                               help = 'Hostname или IP кролика',
                               metavar = 'Rabbit_address')
    unbind_group.add_argument ('-q', '--queue', required=True,
                               help = 'Имя очереди')
    unbind_group.add_argument ('-e', '--exch', required=True,
                               help = 'Имя exchange',
                               metavar = 'Exchange')
    unbind_group.add_argument ('-rk', '--r_key', required=True,
                               help = 'Routing_key для биндинга',
                               metavar = 'Routing_key')

    # Создаем парсер для команды purge
    purge_parser = subparsers.add_parser ('purge',
                                          #add_help = False,
                                          help = 'Режим очистки очереди',
                                          description = '''Запуск в режиме очистки очереди''')
    # Создаем новую группу параметров
    purge_group = purge_parser.add_argument_group (title='Параметры')

    purge_group.add_argument ('-rmq', '--rabbit_address', required=True,
                              help = 'Hostname или IP кролика',
                              metavar = 'Rabbit_address')
    purge_group.add_argument ('-q', '--queue', required=True,
                              help = 'Имя очереди')
    return parser

def time_now():
    return str(datetime.strftime(datetime.now(), "%H:%M:%S"))

def rmq_connect (rabbit_address):
    # устанавливаем соединение с сервером RabbitMQ
    parameters = pika.URLParameters(rmq_params.rabbit_connection_str(rabbit_address))
    print("[" + time_now() + "]", "- Работаем на кролике:", rabbit_address)
    connection = pika.BlockingConnection(parameters)
    print("[" + time_now() + "]", "- Подключение успешно")
    return connection

def rmq_disconnect (connection):
    connection.close()

def create_que (params,rmq_channel):
    try:
        rmq_channel.queue_declare(queue=params.queue, durable = params.durable)
        print("[" + time_now() + "]", "- Очередь", params.queue, "успешно создана")
    except Exception:
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Очередь", params.queue, "уже есть в кролике!")

def create_exch (params,rmq_channel):
    try:
        rmq_channel.exchange_declare(exchange=params.exch, exchange_type = params.type, durable = params.durable)
        print("[" + time_now() + "]", "- Exchange", params.exch, "успешно создан")
    except Exception:
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Exchange", params.exch, "уже есть в кролике!")

def delete (params,rmq_channel):
    if params.queue:
        try:
            rmq_channel.queue_delete(queue=params.queue)
            print("[" + time_now() + "]", "- Очередь", params.queue, "успешно удалена")
        except Exception:
            print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
            print("[" + time_now() + "]", "- Ошибка удаления очереди!")
    elif params.exch:
        try:
            rmq_channel.exchange_delete(exchange=params.exch)
            print("[" + time_now() + "]", "- Exchange", params.exch, "успешно удален")
        except Exception:
            print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
            print("[" + time_now() + "]", "- Ошибка удаления exchange!")

def bind (params,rmq_channel):
    try:
        rmq_channel.queue_bind(exchange=params.exch, queue=params.queue, routing_key=params.r_key)
        print("[" + time_now() + "]", "- Сообщения с routing_key = ",
              params.r_key, "\nуспешно назначены для пересылки из ", params.exch, "\nв очередь", params.queue)
    except Exception:
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Ошибка создания binding!")

def unbind (params,rmq_channel):
    try:
        rmq_channel.queue_unbind(exchange=params.exch, queue=params.queue, routing_key=params.r_key)
        print("[" + time_now() + "]", "- Пересылка сообщений с routing_key = ",
              params.r_key, "\n из ", params.exch, "\nв очередь", params.queue, "успешно прекращена")
    except Exception:
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Ошибка удаления binding!")

def purge (params,rmq_channel):
    try:
        rmq_channel.queue_purge(queue=params.queue)
        print("[" + time_now() + "]", "- Очередь", params.queue, "успешно очищена")
    except Exception:
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Ошибка очистки очереди",params.queue,"!")

parser = createParser()
params = parser.parse_args(sys.argv[1:])

if params.command not in ["create_que","create_exch","delete","bind","unbind","purge"]:
    parser.print_help()
    exit()


rmq_connection = rmq_connect(params.rabbit_address)
rmq_channel = rmq_connection.channel()

if params.command == "create_que":
    create_que(params,rmq_channel)
elif params.command == "create_exch":
    create_exch(params,rmq_channel)
elif params.command == "delete":
    delete(params,rmq_channel)
elif params.command == "bind":
    bind(params,rmq_channel)
elif params.command == "unbind":
    unbind(params,rmq_channel)
elif params.command == "purge":
    purge(params,rmq_channel)
else:
    print("Выбранная команда ничего не делает... Используйте -h для вызова справки")

rmq_disconnect(rmq_connection)