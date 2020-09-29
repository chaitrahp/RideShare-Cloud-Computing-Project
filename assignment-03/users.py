from flask import Flask,render_template,request,jsonify,Response,make_response
import requests
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.sqlite import TIMESTAMP
import csv
import uuid
import sqlite3
import random,re
import datetime,time
import json
from json import dumps
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test4.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
db = SQLAlchemy(app)
from multiprocessing import Value
count = Value('i', 0)

class User(db.Model):
	__tablename__ = "RideShare"
	username = db.Column(db.String(80), unique=True, nullable=False, primary_key=True)
	password = db.Column(db.String(40), nullable=False)
	def __init__(self,username,password):
		self.username = username
		self.password = password


@app.route("/api/v1/users",methods=["POST","DELETE"])
def new_user1():
	with count.get_lock():
		count.value +=1
	return Response("{}", status=405, mimetype='application/json')
#API 1
@app.route("/api/v1/users",methods=["PUT"])
def new_user():
	with count.get_lock():
		count.value +=1
	content = request.get_json()
	pattern = re.compile(r'\b[0-9a-fA-F]{40}\b')
	match = re.match(pattern,content["password"])
	if match == None:
		#return "400"
		return Response(status=400, mimetype='application/json')
	if requests.post("http://3.217.25.245:80/api/v1/db/read",json={"table_name":"RideShare","username":content["username"],"opt":"1"}).text=="1":
		#return "405"
		return Response(status=405, mimetype='application/json')
	else:
		requests.post("http://3.217.25.245:80/api/v1/db/write",json={"table_name":"RideShare","username":content["username"],"password":content["password"],"opt":"1"})
		#return "201"
		return Response(status=201, mimetype='application/json')

@app.route("/api/v1/users/<username>",methods=["PUT","GET","POST"])
def remove_user1():
	with count.get_lock():
		count.value +=1
	return Response("{}", status=405, mimetype='application/json')
#API2
@app.route("/api/v1/users/<username>",methods=["DELETE"])
def remove_user(username):	
	with count.get_lock():
		count.value +=1
	if not (request.method=="DELETE"):
		return Response("{}", status=405, mimetype='application/json')
	#requests.post("http://0.0.0.0:80/api/v1/db/read",json={"table_name":"RideShare","username":username})
	if(requests.post("http://3.217.25.245:80/api/v1/db/read",json={"table_name":"RideShare","username":username,"opt":"1"}).text=="1"):
		a = requests.post("http://3.217.25.245:80/api/v1/db/write",json={"table_name":"RideShare","username":username,"opt":"2"})
		b = requests.post("http://18.215.42.156:80/api/v1/db/write",json={"table_name":"Ride_details","username":username,"opt":"3"})
		c = requests.post("http://18.215.42.156:80/api/v1/db/write",json={"table_name":"Pool_details","username":username,"opt":"3"})
		return Response("{}", status=200, mimetype='application/json')
	else:
		return Response("{}", status=400, mimetype='application/json')


#API9
@app.route("/api/v1/db/read",methods=["POST"])
def read_db():
	content = request.get_json()
	if content["table_name"]=="RideShare":
		if content["opt"]=="1":
			a = User.query.filter_by(username=content["username"]).all()
			if len(a)==0:
				return "0"
			else:
				return "1" 
		elif content["opt"]=="2":
			a = User.query.filter_by().all()
			res={"users":[]}
							
			for r in a:
				res["users"].append(r.username)	
			if(len(res["users"])==0):
				return "0"
			return make_response(dumps(res["users"]))

#API8
@app.route("/api/v1/db/write",methods=["POST"])
def write_db():
	content = request.get_json()
	if(content["table_name"]=="RideShare"):
		if content["opt"]=="1":
			u = User(content["username"],content["password"])
			db.create_all()
			db.session.add(u)
			db.session.commit()
		elif content["opt"]=="2":
			a = User.query.filter_by(username=content["username"]).first()
			db.session.delete(a)
			db.session.commit()
	return "200"


#API11
@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
	if not (request.method=="POST"):
		return Response("{}", status=405, mimetype='application/json')
	meta = db.metadata
	for table in reversed(meta.sorted_tables):
		db.session.execute(table.delete())
	db.session.commit()
	a = requests.post("http://18.215.42.156:80/api/v1/db/clear")
	return Response("{}",status=200, mimetype='application/json')

#API10
@app.route("/api/v1/users",methods=["GET"])
def list_user():
	r = requests.post("http://3.217.25.245:80/api/v1/db/read",json={"table_name":"RideShare","opt":"2"})
	with count.get_lock():
		count.value +=1
	
	if(r.text=="0"):
		return Response("{}", status=204, mimetype='application/json')
	return r.text


@app.route("/api/v1/_count",methods=["GET"])
def count_req():
	d=[]
	d.append(count.value)
	return Response(json.dumps(d),status=200,mimetype='application/json')

@app.route("/api/v1/_count",methods=["DELETE"])
def count_reset():
	with count.get_lock():
		count.value = 0
	return Response({},status=200,mimetype='application/json')
	
@app.route("/api/v1/users/hello",methods=["GET"])
def hello():
	return "600"

if __name__=='__main__':
	db.create_all()
	app.run(host='0.0.0.0',port=80,debug='True')

