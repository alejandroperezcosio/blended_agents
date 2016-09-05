import redis
import ast
import csv
import MySQLdb
from config import Config


config = Config(file('conf.cfg'))

redis_pub_host = config.redis_pub.host
redis_pub_port = config.redis_pub.port
redis_pub_db = config.redis_pub.db
redis_pub_topic = config.redis_pub.topic

redis_agents_host = config.redis_agents.host
redis_agents_port = config.redis_agents.port
redis_agents_db = config.redis_agents.db
redis_agents_key = config.redis_agents.key


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

def get_possible_agents(call_skills):
    skilled_outbound_agents = []
    skilled_inbound_agents = []
    state_available = '3'
    redis_agents = redis.StrictRedis(host=redis_agents_host, port=redis_agents_port, db=redis_agents_db)
    agents = redis_agents.hgetall(redis_agents_key)

    mapped_agents = map(ast.literal_eval, agents.values())

    for agent in mapped_agents:
        agent_skills = set(agent['skills'].split('|'))
        if set(call_skills).issubset(agent_skills):
            if agent['type'] == 'outbound':
                skilled_outbound_agents.append({'name': agent['name'], 'tenant': agent['tenant']})
            elif agent['type'] == 'inbound' and agent['status'] == state_available:
                skilled_inbound_agents.append({'name': agent['name'], 'tenant': agent['tenant']})

    return {'outbound': skilled_outbound_agents, 'inbound': skilled_inbound_agents}

def set_acq_move(db, agents_to_move, tenants):
    cursor = db.cursor()
    for agent in agents_to_move:
        agent = agent['name']
        tenant = tenants[agent['tenant']]
        cursor.execute("UPDATE users SET manual_dial_campaign='%s' WHERE name='%s'" % (tenant, agent))

    db.commit()

def start_call_listener():
    print 'start listening calls'
    redis_subscr = redis.StrictRedis(host=redis_pub_host, port=redis_pub_port, db=redis_pub_db)
    subscription = redis_subscr.pubsub()
    subscription.subscribe(redis_pub_topic)
    tenants = get_tenants_from_csv(config.local_csv.tenants)
    db = MySQLdb.connect(
        host=config.acq_mysql.host,
        user=config.acq_mysql.user,
        passwd=config.acq_mysql.passwd,
        db=config.acq_mysql.db)

    for msg in subscription.listen():
        call_skills = get_skills_from_call(msg)
        if call_skills not in [[], ""]:
            skilled_agents = get_possible_agents(call_skills)
            outbound_agents = skilled_agents['outbound']
            inbound_agents = skilled_agents['inbound']
            if len(inbound_agents) == 0:
                print "skills: %s, outbound agents: %s" % (call_skills, outbound_agents)
                set_acq_move(db, outbound_agents, tenants)

start_call_listener()
