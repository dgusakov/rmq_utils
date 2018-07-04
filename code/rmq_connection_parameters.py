from datetime import datetime

def rabbit_connection_str(adr):
	cp_rabbit = {
			# Задем конект к кролику по IP, %2F - означает vhost = "/"
			'192.168.1.25':'amqp://rabbit_user:rabbit_password@192.168.1.25:5672/%2F',
			# Задем конект к кролику по HOSTNAME
			'my-rmq01':'amqp://rabbit_user:rabbit_pass@my-rmq01:5672/my_vhost',
			}
	while adr not in cp_rabbit and adr != 'exit':
		adr = input('[' + datetime.strftime(datetime.now(), "%H:%M:%S") + '] - Указанный кролик не найден в справочнике!\n Введите корректый адрес сервера RabbitMQ или "exit" для прекращения работы \n')
	if adr == 'exit': exit()
	return cp_rabbit[adr]