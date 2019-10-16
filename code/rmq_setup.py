# -*- coding: utf-8 -*-

import rmq_common_tools as rmq_tools  # конфигурационный файл с параметрами конекта к кроликам
import sys
import argparse  # парсер аргументов командной строки
import traceback  # модуль для вывода трейса ошибки

version = '1.0'


def create_parser():
    # Создаем класс парсера
    parser = argparse.ArgumentParser(prog='RMQ_SETUP_UTILS',
                                     description='''Набор базовых операций для работы с RabbitMQ по протоколу AMQP''',
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
                                       description='Команды, которые должны быть в качестве первого параметра %(prog)s')

    # Создаем парсер для команды create_que
    create_que_parser = subparsers.add_parser('create_que',
                                              # add_help=False,
                                              help='Режим создания очереди',
                                              description='''Запуск в режиме создания очереди''')
    # Создаем новую группу параметров
    create_que_group = create_que_parser.add_argument_group(title='Параметры')

    create_que_group.add_argument('-rmq', '--rabbit_address', required=True,
                                  help='Hostname или IP кролика',
                                  metavar='Rabbit_address')
    create_que_group.add_argument('-q', '--queue', required=True,
                                  help='Имя очереди')
    create_que_group.add_argument('-d', '--durable', action='store_true', default=False,
                                  help='Признак DURABLE для очереди. По умолчанию False')

    # Создаем парсер для команды create_exch
    create_exch_parser = subparsers.add_parser('create_exch',
                                               # add_help=False,
                                               help='Режим создания exchange',
                                               description='''Запуск в режиме создания exchange''')
    # Создаем новую группу параметров
    create_exch_group = create_exch_parser.add_argument_group(title='Параметры')

    create_exch_group.add_argument('-rmq', '--rabbit_address', required=True,
                                   help='Hostname или IP кролика',
                                   metavar='Rabbit_address')
    create_exch_group.add_argument('-e', '--exch', required=True,
                                   help='Имя exchange',
                                   metavar='Exchange')
    create_exch_group.add_argument('-t', '--type', choices=['direct', 'topic', 'fanout', 'headers'], default='topic',
                                   help='Тип создаваемого exchange. По умолчанию Topic')
    create_exch_group.add_argument('-d', '--durable', action='store_true', default=False,
                                   help='Признак DURABLE для exchange. По умолчанию False')

    # Создаем парсер для команды delete
    delete_parser = subparsers.add_parser('delete',
                                          # add_help=False,
                                          help='Режим удаления очереди или exchange',
                                          description='''Запуск в режиме удаления очереди или exchange''')
    # Создаем новую группу параметров
    delete_group = delete_parser.add_argument_group(title='Параметры')

    delete_group.add_argument('-rmq', '--rabbit_address', required=True,
                              help='Hostname или IP кролика',
                              metavar='Rabbit_address')
    delete_group.add_argument('-q', '--queue',
                              help='Имя очереди')
    delete_group.add_argument('-e', '--exch',
                              help='Имя exchange',
                              metavar='Exchange')

    # Создаем парсер для команды bind
    bind_parser = subparsers.add_parser('bind',
                                        # add_help=False,
                                        help='Режим создания binding',
                                        description='''Запуск в режиме создания binding''')
    # Создаем новую группу параметров
    bind_group = bind_parser.add_argument_group(title='Параметры')

    bind_group.add_argument('-rmq', '--rabbit_address', required=True,
                            help='Hostname или IP кролика',
                            metavar='Rabbit_address')
    bind_group.add_argument('-q', '--queue', required=True,
                            help='Имя очереди')
    bind_group.add_argument('-e', '--exch', required=True,
                            help='Имя exchange',
                            metavar='Exchange')
    bind_group.add_argument('-rk', '--r_key', required=True,
                            help='Routing_key для биндинга',
                            metavar='Routing_key')

    # Создаем парсер для команды unbind
    unbind_parser = subparsers.add_parser('unbind',
                                          # add_help=False,
                                          help='Режим удаления binding',
                                          description='''Запуск в режиме удаления binding''')
    # Создаем новую группу параметров
    unbind_group = unbind_parser.add_argument_group(title='Параметры')

    unbind_group.add_argument('-rmq', '--rabbit_address', required=True,
                              help='Hostname или IP кролика',
                              metavar='Rabbit_address')
    unbind_group.add_argument('-q', '--queue', required=True,
                              help='Имя очереди')
    unbind_group.add_argument('-e', '--exch', required=True,
                              help='Имя exchange',
                              metavar='Exchange')
    unbind_group.add_argument('-rk', '--r_key', required=True,
                              help='Routing_key для биндинга',
                              metavar='Routing_key')

    # Создаем парсер для команды purge
    purge_parser = subparsers.add_parser('purge',
                                         # add_help=False,
                                         help='Режим очистки очереди',
                                         description='''Запуск в режиме очистки очереди''')
    # Создаем новую группу параметров
    purge_group = purge_parser.add_argument_group(title='Параметры')

    purge_group.add_argument('-rmq', '--rabbit_address', required=True,
                             help='Hostname или IP кролика',
                             metavar='Rabbit_address')
    purge_group.add_argument('-q', '--queue', required=True,
                             help='Имя очереди')
    return parser


def create_que(params, channel):
    try:
        channel.queue_declare(queue=params.queue, durable=params.durable)
        rmq_tools.console_log("Очередь", params.queue, "успешно создана")
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка создания очереди!")


def create_exch(params, channel):
    try:
        channel.exchange_declare(exchange=params.exch, exchange_type=params.type, durable=params.durable)
        rmq_tools.console_log("Exchange", params.exch, "успешно создан")
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка создания excahange!")


def delete(params, channel):
    if params.queue:
        try:
            channel.queue_delete(queue=params.queue)
            rmq_tools.console_log("Очередь", params.queue, "успешно удалена")
        except Exception:
            rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
            rmq_tools.console_log("Ошибка удаления очереди!")
    elif params.exch:
        try:
            channel.exchange_delete(exchange=params.exch)
            rmq_tools.console_log("Exchange", params.exch, "успешно удален")
        except Exception:
            rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
            rmq_tools.console_log("Ошибка удаления exchange!")


def bind(params, channel):
    try:
        channel.queue_bind(exchange=params.exch, queue=params.queue, routing_key=params.r_key)
        rmq_tools.console_log("Сообщения с routing_key = ",
                              params.r_key, "\nуспешно назначены для пересылки из ", params.exch, "\nв очередь",
                              params.queue)
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка создания binding!")


def unbind(params, channel):
    try:
        channel.queue_unbind(exchange=params.exch, queue=params.queue, routing_key=params.r_key)
        rmq_tools.console_log("Пересылка сообщений с routing_key = ",
                              params.r_key, "\n из ", params.exch, "\nв очередь", params.queue, "успешно прекращена")
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка удаления binding!")


def purge(params, channel):
    try:
        channel.queue_purge(queue=params.queue)
        rmq_tools.console_log("Очередь", params.queue, "успешно очищена")
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка очистки очереди", params.queue, "!")


args_parser = create_parser()
rmq_params = args_parser.parse_args(sys.argv[1:])

if rmq_params.command not in ["create_que", "create_exch", "delete", "bind", "unbind", "purge"]:
    args_parser.print_help()
    exit()

rmq_connection = rmq_tools.rmq_connect(rmq_params.rabbit_address)
rmq_channel = rmq_connection.channel()

if rmq_params.command == "create_que":
    create_que(rmq_params, rmq_channel)
elif rmq_params.command == "create_exch":
    create_exch(rmq_params, rmq_channel)
elif rmq_params.command == "delete":
    delete(rmq_params, rmq_channel)
elif rmq_params.command == "bind":
    bind(rmq_params, rmq_channel)
elif rmq_params.command == "unbind":
    unbind(rmq_params, rmq_channel)
elif rmq_params.command == "purge":
    purge(rmq_params, rmq_channel)
else:
    print("Выбранная команда ничего не делает... Используйте -h для вызова справки")

rmq_tools.rmq_disconnect(rmq_connection)
