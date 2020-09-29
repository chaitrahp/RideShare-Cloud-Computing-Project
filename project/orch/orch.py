import time
from flask import Flask,Response
from flask import request
import requests
from flask import jsonify
from multiprocessing import Value
import logging
import pika
import json
import uuid
import sys
import threading 
import time
import atexit
import math
from apscheduler.schedulers.background import BackgroundScheduler
from kazoo.client import KazooClient
from kazoo.client import KazooState
import docker
app = Flask(__name__)

read_count = Value('i', 0)
q = Value('i',1)
no_slaves = Value('i',1)
first_req = Value('i',0)

#keeping a track of number of read requests and accordingly scaling
def reset_read_count():
	print("******************************** TIMER **************************************************************************",file=sys.stderr)

	count = math.ceil(read_count.value / 20)
	c = 0
	client = docker.from_env()
	logging.basicConfig()
	zk = KazooClient(hosts='zoo:2181')

	zk.start()
	children = zk.get_children("/workers") #, watch=demo_func)
	no-of-children = len(children)
	q = no-of-children
	if(no-of-children < count):
		print("___________________________________________ SCALE UP ____________________________________________________________",file=sys.stderr)
		c = count - no-of-children	
		while c:
			q+=1
			name = "worker_"+ str(q)
			client.containers.run("worker:latest",name=name,links={'rabbitmq':'rabbitmq'},network="newstart_mynet",pid_mode="host")
			c-=1
	
	elif no-of-children > count :
		c = no-of-children - count
		print("___________________________________________ SCALE DOWN ____________________________________________________________",file=sys.stderr)
		while c:
			
			name = "worker_"+str(q)
			rm_cont = client.containers.get(container_id= name)
			rm_cont.remove(force=True)
			q-=1
			c-=1
	with read_count.get_lock():
		read_count.value = 0



#timer, to restart and trigger the scale function every 2 minutes
@app.before_first_request
def timer_req():
	if(True):
		scheduler = BackgroundScheduler()
		scheduler.add_job(func=reset_read_count, trigger="interval", seconds=120)
		scheduler.start()

		# Shut down the scheduler when exiting the app
		atexit.register(lambda: scheduler.shutdown())
			

#slave crash api which gets a list of pids of all the currently running worker containers and crashes the one with highest pid after which zookeeper triggers to create another worker.
@app.route('/api/v1/crash/slave',methods=["POST"])
def crash_slave():
	logging.basicConfig()
	children = requests.get("http://34.193.123.179:80/api/v1/worker/list")
	zk = KazooClient(hosts='zoo:2181')
	zk.start()
	list_children = []
	pid_c = []
	for i in json.loads(children.text):
		list_children.append(int(i))
	if len(list_children)!=0:
		list_children.sort()
		last_child = list_children[len(list_children)-1]
		pid_c.append(str(last_child))
		client = docker.from_env()
		conts = client.containers.list(all=True)
		cli = docker.APIClient(base_url='unix:///var/run/docker.sock')
		for i in conts:
			cont=cli.inspect_container(i.name)
			name=i.name
			if cont['State']['Pid'] == last_child:
				#print("[[[[[[[[[[[ matched ]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]",file=sys.stderr)
				
				rm_cont = client.containers.get(container_id= name)
				
				rm_cont.remove(force=True)
				
				if not(zk.exists("/workers/"+str(x))):
					client.containers.run("worker:latest",name=name,links={'rabbitmq':'rabbitmq'},network="newstart_mynet",pid_mode="host")
				break

	else:
		pid_c= []
	return Response(json.dumps(pid_c),status=200,mimetype='application/json')


#health check	
@app.route('/hello',methods=["GET"])
def health_check():
	#print("HELLOOOOOOO APIIIIIII",file=sys.stderr)
	a = read_count.value
	l = []
	l.append(a)
	return Response(json.dumps(l),status=200,mimetype='application/json')

#to provide response to the read requests
class ResponseQ(object):
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600,blocked_connection_timeout=300))

		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='', exclusive=True)
		self.callback_queue = result.method.queue

		self.channel.basic_consume(queue=self.callback_queue,on_message_callback=self.on_response,auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body

	def call(self,body):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(exchange='',routing_key='read_queue',properties=pika.BasicProperties(reply_to=self.callback_queue,correlation_id=self.corr_id,),body=body)
		while self.response is None:
			self.connection.process_data_events()
		return self.response

#to write after sync		
class WriteQ(object):
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600,blocked_connection_timeout=300))

		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='', exclusive=True)
		self.callback_queue = result.method.queue

		self.channel.basic_consume(queue=self.callback_queue,on_message_callback=self.on_response,auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body

	def call(self,body):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(exchange='',routing_key='write_queue',properties=pika.BasicProperties(reply_to=self.callback_queue,correlation_id=self.corr_id,),body=body)
		while self.response is None:
			self.connection.process_data_events()
		return self.response

@app.route("/api/v1/db/read",methods=["GET"])
def read_db():
	with read_count.get_lock():
		read_count.value +=1
	content = request.get_json()
	pub_q = ResponseQ()
	result = pub_q.call(json.dumps(content))
	return Response(result,status=200,mimetype='application/json')

@app.route("/api/v1/db/write",methods=["POST"])
def write_db():
	content = request.get_json()
	pub_q = WriteQ()
	result = pub_q.call(json.dumps(content))
	return Response(result,status=201,mimetype='application/json')

#to list the pids of all the current worker containers
@app.route("/api/v1/worker/list",methods=["GET"])
def list_worker():
	logging.basicConfig()
	zk = KazooClient(hosts='zoo:2181')
	zk.start()
	zk.ensure_path("/workers")
	children = zk.get_children("/workers")
	return Response(json.dumps(children),status=200,mimetype='application/json')

#to clear the db
class ClearQ(object):

	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600,blocked_connection_timeout=300))
	
		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='',durable=True)
		self.callback_queue =result.method.queue
	
		self.channel.basic_consume(queue=self.callback_queue,on_message_callback=self.on_response,auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body

	def call(self, request):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		#publishing to write_queue
		self.channel.basic_publish(exchange='',routing_key='clear_queue',properties=pika.BasicProperties(reply_to=self.callback_queue,correlation_id=self.corr_id,),body=request)
		while self.response is None:
			self.connection.process_data_events()
		return self.response
		
@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
	content = request.get_json()
	pub_q = ClearQ()
	result = pub_q.call(json.dumps(content))
	return Response(result,status=201,mimetype='application/json')


#this is the orchestrator. Sends the requests to the master and the workers accordingly.	
if  __name__ == '__main__':
	app.run(host='0.0.0.0',port=80,debug=True)
