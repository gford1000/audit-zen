import requests
import json
from uuid import uuid4 as uuid
from datetime import datetime as dt
import random

URL_BASE = "http://localhost:5000"

def process_organisation(data, org_id = None, action='POST'):
	"""Handles registration, updates and deletion of the organisation with the service"""

	url = '/'.join([URL_BASE, "1.0/audit/org/register/"])

	headers = {'content-type': 'application/json'}
	if org_id:
		headers['Audit-Identifier'] = org_id

	if action == 'POST':
		resp = requests.post(url, data=json.dumps(data), headers=headers)
	else:
		resp = requests.delete(url, data=json.dumps(data), headers=headers)

	if resp.status_code == 200:
		print resp.content
		resp_data = json.loads(resp.content)
		return resp_data.get('id', None)
	else:
		raise Exception('Bad processing')

def write_data():
	def get_id():
		return str(uuid())

	def get_tm(d=None):
		if not d:
			d = dt.utcnow()
		return (d.toordinal() * 86400 + d.hour * 3600 + d.minute * 60 + d.second) * 1000000 + d.microsecond

	def random_item(list):
		return list[random.randint(0, len(list)-1)]

	def create_org(num_users):
		org = {}
		org['id'] = get_id()
		org['user_selector'] = random_item
		org['users'] = []
		for x in range(1,num_users):
			org['users'].append(get_id())
		return org

	def create_service_usage(num_orgs, num_users):
		service = {}
		service['id'] = get_id()
		service['org_selector'] = random_item
		service['orgs'] = []
		for x in range(1,num_orgs):
			service['orgs'].append(create_org(num_users))
		return service

	def create_message(service):

		org = service['org_selector'](service['orgs'])
		obo = org['user_selector'](org['users'])
		actor = obo

		if random.randint(0,70) == 40:
			# Sometimes not on own behalf
			actor = org['user_selector'](org['users'])

		return {
			'timestamp': get_tm(),
			'obo_id': obo,
			'actor_id': actor
		}

	url = '/'.join([URL_BASE, "1.0/audit/org/1/services/2/save/"])

	# Dummy some data
	services = []
	for x in range(1,10):
		services.append(create_service_usage(5, 20))


	tm_start = dt.utcnow()
	for x in range(1,2):
		headers = {'content-type': 'application/json'}
		data = create_message(random_item(services))
		print data
		resp = requests.post(url, data=json.dumps(data), headers=headers)
		print resp, resp.content
		if x % 100 == 0:
			tm_end = dt.utcnow()
			print x, get_tm(tm_end) - get_tm(tm_start)
			tm_start = tm_end

if __name__ == "__main__":
	def create_orgs():
		data1 = [
			{
				'name':'XYZ Corp',
				'contact':'fred@xyz.com',
				'website':'www.xyz.com'
			},
			{
				'name':'XYZ Corp',
				'contact':'joe@xyz.com',
				'website':'www.xyz.com'
			},
			{
				'name':'XYZ Corp',
				'contact':'john@xyz.com',
				'website':'www.xyz.com'
			}
		]

		org_id = None
		for d in data1:
			org_id = process_organisation(d, org_id)			

		process_organisation({}, org_id, 'DELETE')

	#write_data()
	create_orgs()



