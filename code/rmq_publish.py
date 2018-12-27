# -*- coding: utf-8 -*-

import rmq_common_tools as rmq_tools  # конфигурационный файл с параметрами конекта к кроликам
import sys
import argparse  # парсер аргументов командной строки
import traceback  # модуль для вывода трейса ошибки

version = '1.0'


def create_parser():
    # Создаем класс парсера
    parser = argparse.ArgumentParser(description='''Утилита для публикации сообщений в RabbitMQ
                                     по протоколу AMQP''',
                                     prog='RMQ_PUBLISH_UTILS',
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

    # Создаем парсер для команды from_console
    from_console_parser = subparsers.add_parser('from_console',
                                                # add_help=False,
                                                help='Режим публикации сообщения введенного через консоль',
                                                description='''Запуск в режиме публикации сообщения
                                                введенного через консоль''')
    # Создаем новую группу параметров
    from_console_group = from_console_parser.add_argument_group(title='Параметры')

    from_console_group.add_argument('-rmq', '--rabbit_address', required=True,
                                    help='Hostname или IP кролика',
                                    metavar='Rabbit_address')
    from_console_group.add_argument('-e', '--exch', required=True,
                                    help='Имя exchange',
                                    metavar='Exchange')
    from_console_group.add_argument('-rk', '--r_key', required=True,
                                    help='Routing_key сообщения',
                                    metavar='Routing_key')
    from_console_group.add_argument('-msg', '--message', required=True,
                                    help='Тело сообщения')
    from_console_group.add_argument('-he', '--header',
                                    help='Заголовок сообщения')

    # Создаем парсер для команды from_file
    from_file_parser = subparsers.add_parser('from_file',
                                             # add_help=False,
                                             help='Режим публикации сообщения из файла',
                                             description='''Запуск в режиме публикации сообщения из файла''')
    # Создаем новую группу параметров
    from_file_group = from_file_parser.add_argument_group(title='Параметры')

    from_file_group.add_argument('-rmq', '--rabbit_address', required=True,
                                 help='Hostname или IP кролика',
                                 metavar='Rabbit_address')
    from_file_group.add_argument('-e', '--exch', required=True,
                                 help='Имя exchange',
                                 metavar='Exchange')
    from_file_group.add_argument('-rk', '--r_key', required=True,
                                 help='Routing_key сообщения',
                                 metavar='Routing_key')
    from_file_group.add_argument('-mf', '--message_file', required=True, type=argparse.FileType(),
                                 help='Имя файла с телом сообщения')
    from_file_group.add_argument('-he', '--header',
                                 help='Заголовок сообщения')

    return parser


def from_console(params, channel):
    try:
        channel.basic_publish(exchange=params.exch, routing_key=params.r_key, body=params.message)
        rmq_tools.console_log("Сообщение: \n", params.message, "\nс routing_key =", params.r_key,
                              "\nуспешно опубликовано в exchange - ", params.exch)
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка публикации сообщения!")


def from_file(params, channel):
    try:
        text = params.message_file.read()
        channel.basic_publish(exchange=params.exch, routing_key=params.r_key, body=text)
        rmq_tools.console_log("Сообщение: \n", text, "\nс routing_key =", params.r_key,
                              "\nуспешно опубликовано в exchange - ", params.exch)
    except Exception:
        rmq_tools.console_log("Ошибка:\n", traceback.format_exc())
        rmq_tools.console_log("Ошибка публикации сообщения!")


args_parser = create_parser()
rmq_params = args_parser.parse_args(sys.argv[1:])

if rmq_params.command not in ["from_console", "from_file"]:
    args_parser.print_help()
    exit()

rmq_connection = rmq_tools.rmq_connect(rmq_params.rabbit_address)
rmq_channel = rmq_connection.channel()

if rmq_params.command == "from_console":
    from_console(rmq_params, rmq_channel)
elif rmq_params.command == "from_file":
    from_file(rmq_params, rmq_channel)
else:
    print("Выбранная команда ничего не делает... Используйте -h для вызова справки")

rmq_tools.rmq_disconnect(rmq_connection)
