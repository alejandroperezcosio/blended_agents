import ast
import csv
import sys
import redis
import signal
import MySQLdb
from config import Config


config = Config(file('conf.cfg'))

redis_calls_host = config.redis_calls.host
redis_calls_port = config.redis_calls.port
redis_calls_db = config.redis_calls.db
redis_calls_topic = config.redis_calls.topic

redis_azs_host = config.redis_azs.host
redis_azs_port = config.redis_azs.port
redis_azs_db = config.redis_azs.db
redis_azs_key = config.redis_azs.key


def get_skills_from_call(msg):
    skills = []
    is_message = msg['type'] == 'message'
    if 'data' in msg and is_message:
        msg_data = ast.literal_eval(msg['data'])
        if 'skills' in msg['data'] and msg_data['action'] == 'adding to queue':
            skills = msg_data['skills'].split(',')

    return skills

def get_tenants_from_csv(csv_file):
    tenants = {}
    csv_file = open(csv_file)
    spamreader = csv.reader(csv_file, delimiter=',', quotechar='\'')

    spamreader.next()
    for row in spamreader:
        tenants[row[0]] = row[5]

    return tenants

def get_skilled_agents(call_skills, redis_azs):
    skilled_outbound_agents = []
    skilled_inbound_agents = []
    state_available = '3'
    agents = redis_azs.hgetall(redis_azs_key)

    mapped_agents = map(ast.literal_eval, agents.values())

    for agent in mapped_agents:
        agent_skills = set(agent['skills'].split('|'))
        if set(call_skills).issubset(agent_skills):
            if agent['type'] == 'outbound':
                skilled_outbound_agents.append({'name': agent['name'], 'tenant': agent['tenant']})
            elif agent['type'] == 'inbound' and agent['status'] == state_available:
                skilled_inbound_agents.append({'name': agent['name'], 'tenant': agent['tenant']})

    return {'outbound': skilled_outbound_agents, 'inbound': skilled_inbound_agents}

def set_acq_move(db, redis_azs, agents_to_move, tenants):
    cursor = db.cursor()
    for agent_dict in agents_to_move:
        agent = agent_dict['name']
        home_campaign = tenants[agent_dict['tenant']]

        cursor.execute("UPDATE users SET manual_dial_campaign='%s' WHERE name='%s'" % (home_campaign, agent))
        db.commit()

        redis_agent = ast.literal_eval(redis_azs.hget(redis_azs_key, agent))
        redis_agent['manual_dial_campaign'] = home_campaign
        redis_azs.hmset(redis_azs_key, {redis_agent['name']: redis_agent})

def start_call_listener():
    print 'start listening calls'
    redis_calls_connection = redis.StrictRedis(host=redis_calls_host, port=redis_calls_port, db=redis_calls_db)
    calls_subscription = redis_calls_connection.pubsub()
    calls_subscription.subscribe(redis_calls_topic)
    redis_azs = redis.StrictRedis(host=redis_azs_host, port=redis_azs_port, db=redis_azs_db)

    tenants = get_tenants_from_csv(config.local_csv.tenants)
    db = MySQLdb.connect(
        host=config.acq_mysql.host,
        user=config.acq_mysql.user,
        passwd=config.acq_mysql.passwd,
        db=config.acq_mysql.db)

    for msg in calls_subscription.listen():
        call_skills = get_skills_from_call(msg)
        if call_skills not in [[], ""]:
            skilled_agents = get_skilled_agents(call_skills, redis_azs)
            outbound_agents = skilled_agents['outbound']
            inbound_agents = skilled_agents['inbound']
            if len(inbound_agents) == 0:
                print "skills: %s, outbound agents: %s" % (call_skills, outbound_agents)
                set_acq_move(db, redis_azs, outbound_agents, tenants)


try:
    start_call_listener()
except KeyboardInterrupt:
    print "\nBye!"
    sys.exit(0)
