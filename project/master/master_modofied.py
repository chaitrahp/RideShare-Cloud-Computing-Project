#!/usr/bin/env python
#from flask import Flask,render_template,request,jsonify,Response,make_response
import requests
#from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.sqlite import TIMESTAMP
from sqlalchemy import create_engine,Column,Integer,String,ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import csv
import uuid
import sqlite3
import random,re
import datetime,time
import json
import pika
from json import dumps
from enum import Enum
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
class Pool(Base):
	__tablename__ = "Pool_details"
	i = Column('i',Integer,primary_key=True,nullable=False)
	ride_id = Column('ride_id',Integer,nullable=False)
	username = Column('username',String) #,primary_key=True)
class Ride(Base):
	__tablename__ = "Ride_details"
	ride_id = Column('ride_id',Integer,primary_key=True,nullable=False)
	created_by = Column('created_by',String,nullable=False)
	timestamp = Column('timestamp',String,nullable=False)
	source = Column('source',Integer,nullable=False)
	destination =Column('destination',Integer,nullable=False)
	'''def __init__(self,created_by,timestamp,source,destination):
		self.created_by = created_by
		#self.timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%y:%S-%M-%H')
		self.timestamp = datetime.datetime.strptime(timestamp, '%d-%m-%Y:%S-%M-%H')
		self.source = source
		self.destination = destination'''

class User(Base):
	__tablename__ = "RideShare"
	username = Column('username',String,primary_key=True)
	#db.Column(db.String(80), unique=True, nullable=False, primary_key=True)
	password = Column('password',String, nullable = False ) 
	#db.Column(db.String(40), nullable=False)
Base.metadata.create_all(bind=engine)


def write(ch, method, props, body):
	content = json.loads(body)
	print("QWERTTTYYY______________________________________________________________________________",body)
	print("QWERTTTYYY______________________________________________________________________________",content)
	if(content["table_name"]=="RideShare"):
		if content["opt"]=="1":
			session = Session()
			u = User()
			u.username = content["username"]
			u.password = content["password"]
			session.add(u)
			session.commit()
			session.close()
			print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ WRITE IT ALREADY @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@") #,db)
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
			u.destinantion = content["destination"]
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

	#ch.exchange_declare(exchange=' ',exchange_type='direct')
	ch.basic_publish(exchange='', routing_key = props.reply_to,properties=pika.BasicProperties(correlation_id=props.correlation_id,delivery_mode=2),body="200")
	ch.basic_ack(delivery_tag=method.delivery_tag)
	
	#update_db_slave(db)
	#ch.exchange_declare(exchange='update',exchange_type='fanout')
	#ch.basic_publish(exchange='update',routing_key='',body=json.dumps(db))
	sync_db_slave(body)
	#ch.exchange_declare(exchange='sync',exchange_type='fanout')
	#ch.basic_publish(exchange='sync', routing_key = '',body=body)
	#ch.basic_ack(delivery_tag=method.delivery_tag)
	
	
	ch.stop_consuming()
	#print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
def sync_db_slave(content):
	print("______________________________sync_db_slave _______________________________________________________________ ")
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq')) 
	channel = connection.channel()
	channel.exchange_declare(exchange='sync', exchange_type='fanout')
	channel.basic_publish(exchange='sync', routing_key='', body=content)
	connection.close()
'''def update_db_slave(db):
	print("______________________________ UPDATE DB SLAVE ______________________________________________________________________________________")
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq')) 
	channel = connection.channel()
	channel.exchange_declare(exchange='update', exchange_type='fanout',durable=True)
	channel.basic_publish(exchange='update', routing_key='', body=json.dumps(db))
	
	#connection.close()'''
def update(ch, method, props, body):
	print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!") #,db)
	db = []
	for i in range(3):
		db.append([])	
	#for table_1
	session=Session()
	t1 = session.query(User).all() 
	t2 = session.query(Pool).all()
	t3 = session.query(Ride).all()
	session.close()	
	for x in t1:
		db[0].append((x.username,x.password))
	for y in t2:
		db[1].append((y.i,y.ride_id,y.username))
	for z in t3:
		db[2].append((z.ride_id,z.created_by,z.timestamp,z.source,z.destination))
	ch.basic_publish(exchange='', routing_key = props.reply_to,properties=pika.BasicProperties(correlation_id=props.correlation_id,delivery_mode=2),body=json.dumps(db))
	ch.basic_ack(delivery_tag=method.delivery_tag)
	ch.stop_consuming()

if __name__=='__main__':
	print("************************************************HEY THERE**********************************************************************")
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq')) 
	write_channel = connection.channel()
	write_channel.queue_declare(queue='write_queue',durable=True)
	write_channel.basic_qos(prefetch_count=1)

	c1 = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
	channel = c1.channel()
	channel.queue_declare(queue='update_queue')
	channel.basic_qos(prefetch_count=1)
	while(1):
		print("================================= IN THE LOOP ===================================================")
		write_channel.basic_consume(queue='write_queue', on_message_callback=write)
		write_channel.start_consuming()
		
		channel.basic_consume(queue='update_queue', on_message_callback=update)
		channel.start_consuming()
		
