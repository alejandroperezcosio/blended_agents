import csv
import sys
import redis
import socket
import random
from config import Config


config = Config(file('conf.cfg'))

csv_file = config.local_csv.agents

socket_host = config.msg_socket.host
socket_port = config.msg_socket.port
socket_tenants = config.msg_socket.tenants
socket_size = config.msg_socket.size
sock = socket.socket()

redis_azs_host = config.redis_azs.host
redis_azs_port = config.redis_azs.port
redis_azs_db = config.redis_azs.db
redis_azs_key = config.redis_azs.key


def connect_socket(host, port, tenants):
    sock.connect((host, port))
    for tenant in tenants:
        sock.send('EA\\TX%s\\TD%s\\LD3\n' % (random.randrange(100,1000), tenant))

    print 'socket connected and listening..'

def get_agents_skills_from_csv(csv_file):
    agents = {}
    csv_file = open(csv_file)
    spamreader = csv.reader(csv_file, delimiter=',', quotechar='\'')

    spamreader.next()
    for row in spamreader:
        agents[row[0]] = row[4]

    return agents

def get_agent_information(az, agents_skills):
    agent = {}
    splitted_az = az.split('\\')

    for param in splitted_az:
        if param.startswith('AN'):
            agent['name'] = param[2:]
            if agent['name'] in agents_skills:
                agent['skills'] = agents_skills[agent['name']]
            else:
                agent = {}
                break
        elif param.startswith('DS'):
            agent['status'] = param[2:]
        elif param.startswith('TD'):
            agent['tenant'] = param[2:]
        elif param.startswith('DM'):
            if param[2:] in ['0','3','4']:
                agent['type'] = 'outbound'
            elif param[2:] in ['5','7']:
                agent['type'] = 'inbound'

    if 'name' not in agent:
        agent = {}

    return agent

def start_az_listener(socket_msg_size):
    agents_skills = get_agents_skills_from_csv(csv_file)
    connect_socket(socket_host, socket_port, socket_tenants)

    redis_azs_connection = redis.StrictRedis(host=redis_azs_host, port=redis_azs_port, db=redis_azs_db)
    redis_azs_connection.delete(redis_azs_key)

    while 1:
        msg = sock.recv(socket_msg_size)
        azs_in_msg = msg.split('\r\n')
        for az in azs_in_msg:
            if az.startswith('AZ'):
                agent = get_agent_information(az, agents_skills)
                if agent != {}:
                    redis_azs_connection.hmset(redis_azs_key, {agent['name']: agent})
                    print agent


try:
    start_az_listener(socket_size)
except KeyboardInterrupt:
    print "\nBye!"
    sys.exit(0)
