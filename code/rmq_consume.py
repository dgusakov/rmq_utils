# -*- coding: utf-8 -*-

import rmq_common_tools as rmq_tools  # конфигурационный файл с параметрами конекта к кроликам
import sys
import argparse  # парсер аргументов командной строки
import traceback  # модуль для вывода трейса ошибки

version = '1.0'


def create_parser():
    # Создаем класс парсера
    parser = argparse.ArgumentParser(prog='RMQ_CONSUMER',
                                     description='''Утилита для считывания сообщений из RabbitMQ 
                                     по протоколу AMQP''',
                                     epilog='''(c) Dmitry.V.Gusakov 2018.
                                     Автор программы, как всегда, не несет никакой ответственности ни за что.''',
                                     add_help=False
                                     )
    # Создаем группу параметров для родительского парсера,
    # ведь у него тоже должен быть параметр --help / -h
    parent_group = parser.add_argument_group(title='Параметры')
    parent_group.add_argument('--help', '-h', action='help', help='Справка')
    parent_group.add_argument('--version',
                              action='version',
                              help='Вывести номер версии',
                              version='%(prog)s {}'.format(version))

    # Создаем группу подпарсеров
    subparsers = parser.add_subparsers(dest='command',
                                       title='Возможные команды',
                                       description='''Команды, которые должны быть переданы 
                                       в качестве первого параметра %(prog)s''')

    # Создаем парсер для команды from_existing_que
    from_existing_que_parser = subparsers.add_parser('from_existing_que',
                                                     # add_help=False,
                                                     help='Запуск процесса вычитки из имеющейся очереди RabbitMQ',
                                                     description='''Запуск процесса вычитки из очереди RabbitMQ.
                                                     Вычитка из имеющейся очереди''')
    # Создаем новую группу параметров
    from_existing_que_group = from_existing_que_parser.add_argument_group(title='Параметры')

    from_existing_que_group.add_argument('-rmq', '--rabbit_address', required=True,
                                         help='Hostname или IP кролика',
                                         metavar='Rabbit_address')
    from_existing_que_group.add_argument('-q', '--queue', required=True,
                                         help='Имя очереди')
    from_existing_que_group.add_argument('-f', '--file', type=argparse.FileType(mode='w'),
                                         default=sys.stdout,
                                         help='Имя файла для записи сообщений')
    from_existing_que_group.add_argument('-c', '--count',
                                         help='''Количество считываемых сообщений. 
                                         После считывания этого количества произойдет автоматическое отключение''')

    # Создаем парсер для команды from_tmp_que
    from_tmp_que_parser = subparsers.add_parser('from_tmp_que',
                                                # add_help=False,
                                                help='Запуск процесса вычитки из временной очереди RabbitMQ',
                                                description='''Запуск процесса вычитки из очереди RabbitMQ.
                                                 Создание и вычитка из временной очереди''')
    # Создаем новую группу параметров
    from_tmp_que_group = from_tmp_que_parser.add_argument_group(title='Параметры')

    from_tmp_que_group.add_argument('-rmq', '--rabbit_address', required=True,
                                    help='Hostname или IP кролика',
                                    metavar='Rabbit_address')
    from_tmp_que_group.add_argument('-q', '--queue', required=True,
                                    help='Имя очереди')
    from_tmp_que_group.add_argument('-e', '--exch', required=True,
                                    help='Имя exchange для биндинга',
                                    metavar='Exchange')
    from_tmp_que_group.add_argument('-rk', '--r_key', required=True,
                                    help='Routing_key для биндинга',
                                    metavar='Routing_key')
    from_tmp_que_group.add_argument('-f', '--file', type=argparse.FileType(mode='w'),
                                    default=sys.stdout,
                                    help='Имя файла для записи сообщений')
    from_tmp_que_group.add_argument('-c', '--count',
                                    help='''Количество считываемых сообщений. 
                                    После считывания этого количества произойдет автоматическое отключение''')

    return parser


def on_message(channel, method_frame, header_frame, body):
    """Обработчик для считанных из "кролика" сообщений. Записывает полученное сообщение в файл или выводит в консоль

    Args:
         channel:       объект соединения с RabbitMQ
         method_frame:  объект с служебными параметрами сообщения из кролика
         header_frame:  объект, содержащий заголовок сообщения
         body:          тело сообщения
    """
    global all_cnt, lim
    if all_cnt >= lim:
        rmq_tools.console_log('Достаточное количество информации собрано.')
        raise KeyboardInterrupt
    body_str = body.decode("utf-8")[:4000]
    rkey = method_frame.routing_key
    cmd_line_arguments.file.write(rkey + '\n')
    cmd_line_arguments.file.write(body_str + '\n\n')
    all_cnt = all_cnt + 1
    if (lim != 0) and (cmd_line_arguments.file == sys.stdout):
        sys.stdout.write(f'[{rmq_tools.time_now()}] - {all_cnt} of {lim} messages consumed.\r')
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


def create_que(params, channel):
    try:
        channel.queue_declare(queue=params.queue)
        rmq_tools.console_log("Очередь", params.queue, "успешно создана")
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Очередь", params.queue, "уже есть в кролике!")
        rmq_tools.console_log("Проверьте параметры и запустите программу повторно")
        exit()


def bind(params, channel):
    try:
        channel.queue_bind(exchange=params.exch, queue=params.queue, routing_key=params.r_key)
        rmq_tools.console_log("Сообщения с routing_key = ",
                              params.r_key, "\nуспешно назначены для пересылки из ",
                              params.exch, "\nв очередь", params.queue)
    except Exception:
        delete(params, channel)
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка создания binding!")
        rmq_tools.console_log("Проверьте параметры и запустите программу повторно")
        exit()


def purge(params, channel):
    try:
        channel.queue_purge(queue=params.queue)
        rmq_tools.console_log("Очередь", params.queue, "успешно очищена")
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка очистки очереди", params.queue, "!")


def delete(params, channel):
    try:
        channel.queue_delete(queue=params.queue)
        rmq_tools.console_log("Очередь", params.queue, "успешно удалена")
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка удаления очереди!")


def from_existing_que(params, channel):
    rmq_tools.console_log("Начитаем считывание.")
    channel.basic_consume(on_message, queue=params.queue)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    except Exception:
        channel.stop_consuming()
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Consumer аварийно завершил работу! Обратитесь к разработчику для устранения проблемы.")


def from_tmp_que(params, channel):
    create_que(params, channel)
    bind(params, channel)
    from_existing_que(params, channel)
    purge(params, channel)
    delete(params, channel)


args_parser = create_parser()
cmd_line_arguments = args_parser.parse_args(sys.argv[1:])

if cmd_line_arguments.command not in ["from_existing_que", "from_tmp_que"]:
    args_parser.print_help()
    exit()

rmq_connection = rmq_tools.rmq_connect(cmd_line_arguments.rabbit_address)
rmq_channel = rmq_connection.channel()

if cmd_line_arguments.count:
    lim = int(cmd_line_arguments.count)
else:
    lim = 0

all_cnt = 0

if cmd_line_arguments.command == "from_existing_que":
    from_existing_que(cmd_line_arguments, rmq_channel)
elif cmd_line_arguments.command == "from_tmp_que":
    from_tmp_que(cmd_line_arguments, rmq_channel)
else:
    print("Выбранная команда ничего не делает... Используйте -h для вызова справки")

rmq_tools.rmq_disconnect(rmq_connection)
