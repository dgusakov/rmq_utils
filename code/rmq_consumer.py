#подключаем необходимые библиотеки

import pika                             		# библиотека для работы с AMQP протоколом
import rmq_connection_parameters as rmq_params 	# конфигурационный файл с параметрами конекта к кроликам
from datetime import datetime           		# библиотека для вывода времени в консоль
import sys
import argparse                         		# парсер аргументов командной строки
import traceback                        		# модуль для вывода трейса ошибки

version = "0.2"

### 0.1 - Первый билд
### 0.2 - Добавлены обработчикм исключений при считывании сообщений

def createParser ():
    # Создаем класс парсера
    parser = argparse.ArgumentParser(prog = 'RMQ_CONSUMER',
                                     description = '''Утилита для считывания сообщений из RabbitMQ по протоколу AMQP''',
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

    # Создаем парсер для команды from_existing_que
    from_existing_que_parser = subparsers.add_parser ('from_existing_que',
                                                      #add_help = False,
                                                      help = 'Запуск процесса вычитки из имеющейся очереди RabbitMQ',
                                                      description = '''Запуск процесса вычитки из очереди RabbitMQ.
                                                      Вычитка из имеющейся очереди''')
    # Создаем новую группу параметров
    from_existing_que_group = from_existing_que_parser.add_argument_group (title='Параметры')

    from_existing_que_group.add_argument ('-rmq', '--rabbit_address', required=True,
                                          help = 'Hostname или IP кролика',
                                          metavar = 'Rabbit_address')
    from_existing_que_group.add_argument ('-q', '--queue', required=True,
                                          help = 'Имя очереди')
    from_existing_que_group.add_argument ('-f', '--file', type=argparse.FileType(mode='w'),
                                          default=sys.stdout,
                                          help = 'Имя файла для записи сообщений')
    from_existing_que_group.add_argument ('-c', '--count',
                                          help = 'Количество считываемых сообщений. После считывания этого количества произойдет автоматическое отключение')

    # Создаем парсер для команды from_tmp_que
    from_tmp_que_parser = subparsers.add_parser ('from_tmp_que',
                                                 #add_help = False,
                                                 help = 'Запуск процесса вычитки из временной очереди RabbitMQ',
                                                 description = '''Запуск процесса вычитки из очереди RabbitMQ.
                                                 Создание и вычитка из временной очереди''')
    # Создаем новую группу параметров
    from_tmp_que_group = from_tmp_que_parser.add_argument_group (title='Параметры')

    from_tmp_que_group.add_argument ('-rmq', '--rabbit_address', required=True,
                                     help = 'Hostname или IP кролика',
                                     metavar = 'Rabbit_address')
    from_tmp_que_group.add_argument ('-q', '--queue', required=True,
                                     help = 'Имя очереди')
    from_tmp_que_group.add_argument ('-e', '--exch', required=True,
                                     help = 'Имя exchange для биндинга',
                                     metavar = 'Exchange')
    from_tmp_que_group.add_argument ('-rk', '--r_key', required=True,
                                     help = 'Routing_key для биндинга',
                                     metavar = 'Routing_key')
    from_tmp_que_group.add_argument ('-f', '--file', type=argparse.FileType(mode='w'),
                                     default=sys.stdout,
                                     help = 'Имя файла для записи сообщений')
    from_tmp_que_group.add_argument ('-c', '--count',
                                     help = 'Количество считываемых сообщений. После считывания этого количества произойдет автоматическое отключение')

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

def on_message(channel, method_frame, header_frame, body):
    global cnt, all, lim
    if all >= lim:
        print('[' + time_now() + '] - Достаточное количество информации собранно.')
        raise KeyboardInterrupt
    body_str = body.decode("utf-8")[:4000]
    rk = method_frame.routing_key
    params.file.write(rk + '\n')
    params.file.write(body_str + '\n\n')
    all = all + 1
    if (lim != 0) and (params.file != sys.stdout):
        sys.stdout.write('[' + time_now() + '] - ' + str(all) + ' of ' + str(lim) + ' messages consumed. \r')
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

def create_que (params,rmq_channel):
    try:
        rmq_channel.queue_declare(queue=params.queue)
        print("[" + time_now() + "]", "- Очередь", params.queue, "успешно создана")
    except Exception:
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Очередь", params.queue, "уже есть в кролике!")
        print("[" + time_now() + "]", "- Проверьте параметры и запустите программу повторно")
        exit()

def bind (params,rmq_channel):
    try:
        rmq_channel.queue_bind(exchange=params.exch, queue=params.queue, routing_key=params.r_key)
        print("[" + time_now() + "]", "- Сообщения с routing_key = ",
              params.r_key, "\nуспешно назначены для пересылки из ", params.exch, "\nв очередь", params.queue)
    except Exception:
        delete(params,rmq_channel)
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Ошибка создания binding!")
        print("[" + time_now() + "]", "- Проверьте параметры и запустите программу повторно")
        exit()

def purge (params,rmq_channel):
    try:
        rmq_channel.queue_purge(queue=params.queue)
        print("[" + time_now() + "]", "- Очередь", params.queue, "успешно очищена")
    except Exception:
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Ошибка очистки очереди",params.queue,"!")

def delete (params,rmq_channel):
    try:
        rmq_channel.queue_delete(queue=params.queue)
        print("[" + time_now() + "]", "- Очередь", params.queue, "успешно удалена")
    except Exception:
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Ошибка удаления очереди!")

def from_existing_que (params,rmq_channel):
    print('[' + time_now() + '] - Начитаем считывание.')
    rmq_channel.basic_consume(on_message, queue = params.queue)
    try:
        rmq_channel.start_consuming()
    except KeyboardInterrupt:
        rmq_channel.stop_consuming()
    except Exeption:
        rmq_channel.stop_consuming()
        print("[" + time_now() + "]"," - Ошибка:\n", traceback.format_exc())
        print("[" + time_now() + "]", "- Consumer аварийно завершил работу! Обратитесь к разработчику для устранения проблемы.")

def from_tmp_que (params,rmq_channel):
    create_que(params,rmq_channel)
    bind(params,rmq_channel)
    from_existing_que(params,rmq_channel)
    purge(params,rmq_channel)
    delete(params,rmq_channel)


parser = createParser()
params = parser.parse_args(sys.argv[1:])

if params.command not in ["from_existing_que","from_tmp_que"]:
    parser.print_help()
    exit()

rmq_connection = rmq_connect(params.rabbit_address)
rmq_channel = rmq_connection.channel()

if params.count:
    lim = int(params.count)
else:
    lim = 0

all = 0

if params.command == "from_existing_que":
    from_existing_que(params,rmq_channel)
elif params.command == "from_tmp_que":
    from_tmp_que(params,rmq_channel)
else:
    print("Выбранная команда ничего не делает... Используйте -h для вызова справки")

rmq_disconnect(rmq_connection)