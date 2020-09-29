
from flask import Flask,render_template,request,jsonify,Response,make_response
import requests
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.sqlite import TIMESTAMP
import csv
import uuid
import sqlite3
import random,re
import datetime,time

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test4.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
db = SQLAlchemy(app)

class User(db.Model):
	__tablename__ = "RideShare"
	username = db.Column(db.String(80), unique=True, nullable=False, primary_key=True)
	password = db.Column(db.String(40), nullable=False)
	def __init__(self,username,password):
		self.username = username
		self.password = password
#API 1
@app.route("/api/v1/users",methods=["PUT"])
def new_user():
	#if not (request.method=="PUT"):
	#	return Response("{}", status=405, mimetype='application/json')
	content = request.get_json()
	pattern = re.compile(r'\b[0-9a-fA-F]{40}\b')
	match = re.match(pattern,content["password"])
	if match == None:
		#return "400"
		return Response(status=400, mimetype='application/json')
	if requests.post("http://34.197.178.71:8080/api/v1/db/read",json={"table_name":"RideShare","username":content["username"]}).text=="1":
		#return "405"
		return Response(status=405, mimetype='application/json')
	else:
		requests.post("http://34.197.178.71:8080/api/v1/db/write",json={"table_name":"RideShare","username":content["username"],"password":content["password"],"opt":"1"})
		#return "201"
		return Response(status=201, mimetype='application/json')

#API2
@app.route("/api/v1/users/<username>",methods=["DELETE"])
def remove_user(username):	
	if not (request.method=="DELETE"):
		return Response("{}", status=405, mimetype='application/json')
	#requests.post("http://0.0.0.0:80/api/v1/db/read",json={"table_name":"RideShare","username":username})
	if(requests.post("http://34.197.178.71:8080/api/v1/db/read",json={"table_name":"RideShare","username":username}).text=="1"):
		a = requests.post("http://34.197.178.71:8080/api/v1/db/write",json={"table_name":"RideShare","username":username,"opt":"2"})
		b = requests.post("http://rides:8080/api/v1/db/write",json={"table_name":"Ride_details","username":username,"opt":"3"})
		c = requests.post("http://rides:8080/api/v1/db/write",json={"table_name":"Pool_details","username":username,"opt":"3"})
		return Response("{}", status=200, mimetype='application/json')
	else:
		return Response("{}", status=400, mimetype='application/json')


#API9
@app.route("/api/v1/db/read",methods=["POST"])
def read_db():
	content = request.get_json()
	if content["table_name"]=="RideShare":
		#return "2"
		#a = db.session.query(db.exists().where(User.username==content["username"])).scalar()
		a = User.query.filter_by(username=content["username"]).all()
		if len(a)==0:
			return "0"
		else:
			return "1" 

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
	a = requests.post("http://rides:8000/api/v1/db/clear")
	return Response("{}",status=200, mimetype='application/json')

#API10
@app.route("/api/v1/users",methods=["GET"])
def list_user():
	if not (request.method=="GET"):
		return Response("{}", status=405, mimetype='application/json')
	a = User.query.all()
	if len(a)==0:
		#return "0"
		return Response("{}", status=200, mimetype='application/json')
	l = []
	for i in a:
		l.append(i.username)
	return jsonify(l)


if __name__=='__main__':
	db.create_all()
	app.run(host='0.0.0.0',port=8080,debug='True')

