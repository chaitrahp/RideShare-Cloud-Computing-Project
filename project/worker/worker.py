import requests
import os
from sqlalchemy.dialects.sqlite import TIMESTAMP
from sqlalchemy import create_engine,Column,Integer,String,ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import logging
from kazoo.client import KazooClient
from kazoo.client import KazooState
import csv
import uuid
import sqlite3
import random,re
import datetime,time
import json
import pika
import os
from json import dumps
from enum import Enum
import docker
import threading 
class Constants(Enum):
	KempegowdaWard=1
	ChowdeshwariWard=2
	Atturu=3
	YelahankaSatelliteTown=4
	Jakkuru=5
	Thanisandra=6
	Byatarayanapura=7
	Kodigehalli=8
	Vidyaranyapura=9
	DoddaBommasandra=10
	KuvempuNagar=11
	Shettihalli=12
	Mallasandra=13
	Bagalakunte=14
	TDasarahalli=15
	Jalahalli=16
	JPPark=17
	RadhakrishnaTempleWard=18
	SanJayanagar=19
	GangaNagar=20
	Hebbala=21
	VishwanathNagenhalli=22
	Nagavara=23
	HBRLayout=24
	Horamavu=25
	RamamurthyNagar=26
	Banasavadi=27
	Kammanahalli=28
	Kacharkanahalli=29
	Kadugondanahalli=30
	KushalNagar=31
	KavalBairasandra=32
	ManorayanaPalya=33
	Gangenahalli=34
	AramaneNagara=35
	Mattikere=36
	Yeshwanthpura=37
	HMTWard=38
	Chokkasandra=39
	DoddaBidarakallu=40
	PeenyaIndustrialArea=41
	LakshmiDeviNagar=42
	NandiniLayout=43
	MarappanaPalya=44
	Malleshwaram=45
	JayachamarajendraNagar=46
	DevaraJeevanahalli=47
	MuneshwaraNagar=48
	Lingarajapura=49
	Benniganahalli=50
	Vijnanapura=51
	KRPuram=52
	Basavanapura=53
	Hudi=54
	Devasandra=55
	ANarayanapura=56
	CVRamanNagar=57
	NewTippaSandra=58
	MaruthiSevaNagar=59
	SagayaraPuram=60
	SKGarden=61
	RamaswamyPalya=62
	JayaMahal=63
	RajMahalGuttahalli=64
	KaduMalleshwarWard=65
	SubramanyaNagar=66
	Nagapura=67
	Mahalakshmipuram=68
	Laggere=69
	RajagopalNagar=70
	Hegganahalli=71
	Herohalli=72
	Kottegepalya=73
	ShakthiGanapathiNagar=74
	ShankarMatt=75
	GayithriNagar=76
	DattatreyaTempleWard=77
	PulakeshiNagar=78
	SarvagnaNagar=79
	HoysalaNagar=80
	VijnanaNagar=81
	GarudacharPalya=82
	Kadugodi=83
	Hagadur=84
	DoddaNekkundi=85
	Marathahalli=86
	HALAirport=87
	JeevanbhimaNagar=88
	Jogupalya=89
	Halsoor=90
	BharathiNagar=91
	ShivajiNagar=92
	VasanthNagar=93
	GandhiNagar=94
	SubhashNagar=95
	Okalipuram=96
	DayanandaNagar=97
	PrakashNagar=98
	RajajiNagar=99
	BasaveshwaraNagar=100
	Kamakshipalya=101
	VrisahbhavathiNagar=102
	Kaveripura=103
	GovindarajaNagar=104
	AgraharaDasarahalli=105
	DrRajKumarWard=106
	ShivaNagar=107
	SriRamaMandirWard=108
	Chickpete=109
	SampangiramNagar=110
	ShantalaNagar=111
	Domlur=112
	KonenaAgrahara=113
	Agaram=114
	VannarPet=115
	Nilasandra=116
	ShanthiNagar=117
	SudhamNagar=118
	DharmarayaSwamyTemple=119
	Cottonpete=120
	BinniPete=121
	KempapuraAgrahara=122
	ViJayanagar=123
	Hosahalli=124
	Marenahalli=125
	MaruthiMandirWard=126
	Mudalapalya=127
	Nagarabhavi=128
	JnanaBharathiWard=129
	Ullalu=130
	Nayandahalli=131
	Attiguppe=132
	HampiNagar=133
	BapujiNagar=134
	Padarayanapura=135
	JagajivanaramNagar=136
	Rayapuram=137
	ChelavadiPalya=138
	KRMarket=139
	ChamrajaPet=140
	AzadNagar=141
	Sunkenahalli=142
	VishveshwaraPuram=143
	Siddapura=144
	HombegowdaNagar=145
	Lakkasandra=146
	Adugodi=147
	Ejipura=148
	Varthur=149
	Bellanduru=150
	Koramangala=151
	SuddaguntePalya=152
	Jayanagar=153
	Basavanagudi=154
	HanumanthNagar=155
	SriNagar=156
	GaliAnjenayaTempleWard=157
	DeepanjaliNagar=158
	Kengeri=159
	RajaRajeshwariNagar=160
	Hosakerehalli=161
	GiriNagar=162
	Katriguppe=163
	VidyaPeetaWard=164
	GaneshMandirWard=165
	KariSandra=166
	Yediyur=167
	PattabhiRamNagar=168
	ByraSandra=169
	JayanagarEast=170
	GurappanaPalya=171
	Madivala=172
	JakkaSandra=173
	HSRLayout=174
	Bommanahalli=175
	BTMLayout=176
	JPNagar=177
	Sarakki=178
	ShakambariNarar=179
	BanashankariTempleWard=180
	KumaraSwamyLayout=181
	PadmanabhaNagar=182
	ChikkalaSandra=183
	Uttarahalli=184
	Yelchenahalli=185
	Jaraganahalli=186
	Puttenahalli=187
	Bilekhalli=188
	HongaSandra=189
	MangammanaPalya=190
	SingaSandra=191
	Begur=192
	Arakere=193
	Gottigere=194
	Konankunte=195
	Anjanapura=196
	Vasanthpura=197
	Hemmigepura=198
Base = declarative_base()
engine = create_engine('sqlite:///test.db',echo=True)
Session = sessionmaker(bind=engine)

#db class creation
class Pool(Base):
	__tablename__ = "Pool_details"
	i = Column('i',Integer,primary_key=True,nullable=False)
	ride_id = Column('ride_id',Integer,nullable=False)
	username = Column('username',String)
	
class Ride(Base):
	__tablename__ = "Ride_details"
	ride_id = Column('ride_id',Integer,primary_key=True,nullable=False)
	created_by = Column('created_by',String,nullable=False)
	timestamp = Column('timestamp',String,nullable=False)
	source = Column('source',Integer,nullable=False)
	destination =Column('destination',Integer,nullable=False)

class User(Base):
	__tablename__ = "RideShare"
	username = Column('username',String,primary_key=True)
	password = Column('password',String, nullable = False ) 
Base.metadata.create_all(bind=engine)

#db read mechanism
def read(ch, method, props, body):
	content =json.loads(body)
	if content["table_name"]=="RideShare":
		if content["opt"]=="1":
			session = Session()
			user_list = session.query(User).filter(User.username==content["username"]).all()
			session.close()
			z = []
			for x in user_list:
				z.append(x)
			if len(z)==0:
				response = "0"
			else:
				response = "1"
		elif content["opt"]=="2":
			session = Session()
			a = session.query(User).filter().all()
			res={"users":[]}

			for r in a:
				res["users"].append(r.username)	
			if(len(res["users"])==0):
				response = "0"
		
			else:
				response = dumps(res["users"])
				print("*****************LISTTTTTTTT OF USERS******************************************************** ",res["users"])
			session.close()
	elif content["table_name"]=="Ride_details":
		if content["opt"]=="1":
			l={}
			v={}
			i=0
			t=datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y:%S-%M-%H')
			x=datetime.datetime.strptime(t, '%d-%m-%Y:%S-%M-%H')
			session = Session()
			rides = session.query(Ride).filter(Ride.timestamp>=x,Ride.source==content["source"],Ride.destination==content["destination"]).all()
			for r in rides:
				name1=[]
				qwerty = session.query(User).all()
				for entry in qwerty:
					name1.append(entry.username)
				
				a = False
				
				if len(name1) == 0:
					a = False
				
				for x in name1:
					if (r.created_by==x):	
						a = True
				if a:
					abc=datetime.datetime.strptime(r.timestamp,'%Y-%m-%d %H:%M:%S')
					time_x=abc.strftime('%d-%m-%Y:%S-%M-%H')
					l[i]={"rideId":r.ride_id,"username":r.created_by,"timestamp":time_x}
					i+=1
			if len(l)==0:
				respone = "0"
			else:
				
				yup = list(l.values())
				response = dumps(yup)
			session.close()

		elif content["opt"]=="2":
			
			session = Session()
			a = session.query(Ride).filter(Ride.source==content["source"],Ride.destination==content["destination"]).all()
			l = []
			for y in a:
				l.append(y)
			if len(l)==0:
				response =  "0"
			else:
				response = "1"
			session.close()
			
		elif content["opt"]=="3":
				
				session = Session()
				a = session.query(Ride).filter(Ride.ride_id==content["ride_id"]).all()
				l = []
				for y in a:
					l.append(y)
				if len(l)!=0:
					response = "1"
				else:
					response = "0"
				session.close()
		elif content["opt"]=="4":
			session = Session()
			q  = session.query(Ride).filter(Ride.ride_id==content["ride_id"]).first()
			response = str(q.created_by)
			session.close()
			
		elif content["opt"]=="5":
			session = Session()
			l=[None]*4
			entry = session.query(Ride).filter(Rideride_id==content["ride_id"]).first()
			l[0]  =entry.timestamp
			l[1] = entry.source
			l[2] = entry.destination
			l[3] = entry.created_by
			session.close()
			response = json.dumps(l)

	elif content["table_name"]=="Pool_details":
		if content["opt"]=="2": #user shouldnt already be in POOL for THAT ride
			session = Session()
			a=session.query(Pool).filter(Pool.ride_id==content["ride_id"],Ride.username==content["username"]).all()
			l = []
			for y in a:
				l.append(y)
			if len(l)!=0:
				response = "0"
			else:
				response = "1"
			session.close()
		if content["opt"]=="3":
			session = Session()
			rows  = session.query(Pool).filter(Pool.ride_id==content["ride_id"]).all()
			l = []
			for i in rows:
				l.append(i.username)
			response =  json.dumps(l)
			session.close()
		elif content["opt"]=="1":
			session = Session()
			a = session.query(Pool).filter(Pool.ride_id==content["ride_id"]).all()
			l = []
			for y in a:
				l.append(y)
			if len(l)!=0:
				response = "1"
			else:
				response = "0"

	ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = props.correlation_id),body=response)
	ch.basic_ack(delivery_tag=method.delivery_tag)
	ch.stop_consuming()

#db write mechanism
def write(ch, method, props, body):
	content = json.loads(body)

	if(content["table_name"]=="RideShare"):
		if content["opt"]=="1":
			session = Session()
			u = User()
			u.username = content["username"]
			u.password = content["password"]
			
			session.add(u)
			session.commit()
			session.close()
			
		elif content["opt"]=="2":
			session = Session()
			a = session.query(User).filter(User.username==content["username"]).first()
			
			session.delete(a)
			session.commit()
			session.close()
	
	elif(content["table_name"]=="Ride_details"):
		if content["opt"]=="1": #delete ride
			session = Session()
			a = session.query(Ride).filter(Ride.ride_id==content["ride_id"]).all()
			for r in a:
				session.delete(r)
				session.commit()
			session.close()
		elif content["opt"]=="2":
			session = Session()
			u = Ride()
			u.created_by = content["created_by"]
			u.source = content["source"]
			u.timestamp = datetime.datetime.strptime(content["timestamp"], '%d-%m-%Y:%S-%M-%H')
			u.destination = content["destination"]
			session.add(u)
			session.commit()
			session.close()
		elif content["opt"]=="3":
			session = Session()
			b = session.query(Ride).filter(Ride.created_by==content["username"]).all()
			for b1 in b:
				session.delete(b1)
				session.commit()
			session.close()
	elif(content["table_name"]=="Pool_details"):
		if content["opt"]=="1": #delete ride
			session = Session()
			d = session.query(Pool).filter(Pool.ride_id==content["ride_id"]).all()
			for r in d:
				session.delete(r)
				session.commit()
		elif content["opt"]=="2":
			session = Session()
			u = Pool()
			u.ride_id = content["ride_id"]
			u.username = content["username"]
			session.add(u)
			session.commit()
			session.close()
		elif content["opt"]=="3":
			session = Session()
			c =session.query(Pool).filter(Pool.username==content["username"]).all()
			for c1 in c:
				session.delete(c1)
				session.commit()
			session.close()
	ch.stop_consuming()


#When master writes into database, the sync/update has to happen to slave nodes which is done here
class UpdateQ(object):
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600,blocked_connection_timeout=300))

		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='', durable=True)
		self.callback_queue = result.method.queue

		self.channel.basic_consume(queue=self.callback_queue,on_message_callback=self.on_response,auto_ack=True)
		

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body
			
	def call(self,body):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(exchange= '',routing_key='update_queue',properties=pika.BasicProperties(reply_to=self.callback_queue,correlation_id=self.corr_id,),body=body)
		while self.response is None:
			self.connection.process_data_events()
		return self.response
		
def update_db():
	
	req = UpdateQ()
	body = "1"
	response = req.call(body)
	tables = json.loads(response)
	t1 = tables[0]
	t2 = tables[1]
	t3 = tables[2]
	session = Session()
	for tup in t1:
		u = User()
		u.username = tup[0]
		u.password = tup[1]
		session.add(u)
		session.commit()
	for tup in t2:
		u = Pool()
		u.i = tup[0]
		u.ride_id = tup[1]
		u.username = tup[2]
		session.add(u)
		session.commit()
	for tup in t3:
		u = Ride()
		u.ride_id	= tup[0]
		u.created_by = tup[1]
		u.timestamp = tup[2]
		u.source = tup[3]
		u.destination = tup[4]
		session.add(u)
		session.commit()
	session.close()
	
#to clear the db	
def clear(ch, method, properties, body):
	session = Session()
	a = session.query(User).all()
	for x in a:
		session.delete(x)
		session.commit()
	
	a = session.query(Pool).all()
	for x in a:
		session.delete(x)
		session.commit()
	a = session.query(Ride).all()
	for x in a:
		session.delete(x)
		session.commit()
	session.close()
	ch.stop_consuming()

#to keep watch 
def demo_func(event):
	print(">>>>>>>>>>>>>>>>>>> DEMO FUNCTION TRig>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	#client = docker.from_env()
	#logging.basicConfig()
	#zk = KazooClient(hosts='zoo:2181')
	#zk.start()
	children = zk.get_children("/workers") #, watch=demo_func)
	xar = len(children)
	q = xar +1
	name = "worker_"+ str(q)
	#client.containers.run("worker:latest",name=name,links={'rabbitmq':'rabbitmq'},detach= True,network="newstart_mynet",pid_mode="host")


#zookeeper and channels listening
if __name__ == '__main__':
	logging.basicConfig()
	zk = KazooClient(hosts='zoo:2181')
	zk.start()
	zk.ensure_path("/workers")
	pid = str(os.getppid())
	if not(zk.exists("/workers/"+pid)):
		zk.create("/workers/"+pid,ephemeral=True)
	update_db()
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600,blocked_connection_timeout=300))
	read_channel = connection.channel()
	read_channel.queue_declare(queue='read_queue')
	read_channel.basic_qos(prefetch_count=1)
	
	c2 = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600,blocked_connection_timeout=300))
	channel = c2.channel()
	channel.exchange_declare(exchange='sync_clear', exchange_type='fanout')
	r = channel.queue_declare(queue='', durable=True)
	qn = r.method.queue
	channel.queue_bind(exchange='sync_clear', queue=qn)

	c1 = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600,blocked_connection_timeout=300))
	write_channel = c1.channel()
	write_channel.exchange_declare(exchange='sync', exchange_type='fanout')
	
	
	result = write_channel.queue_declare(queue='', durable=True)
	queue_name = result.method.queue
	write_channel.queue_bind(exchange='sync', queue=queue_name)


	while 1:
		mf1, hf1, b1 = write_channel.basic_get(queue_name,auto_ack=True)
		if mf1:
			#print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^Message is present ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
			write(write_channel,mf1, hf1, b1)
		
		mf2, hf2, b2 = read_channel.basic_get('read_queue')
		if mf2:
			#print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^Message is present ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
			read(read_channel,mf2, hf2, b2)

		mf3, hf3, b3 = channel.basic_get(qn,auto_ack=True)
		if mf3:
			#print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^Message is present ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
			clear(channel,mf3, hf3, b3)