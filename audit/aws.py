from boto.dynamodb2 import regions
from boto.dynamodb2.table import Table
from boto.dynamodb2.layer1 import DynamoDBConnection
from audit import get_tm, create_hash
from uuid import uuid4 as uuid

class ValidationError(Exception):
	pass

class SaveError(Exception):
	pass

class Audit(object):
	"""
	Provides all the functionality required to validate and save data into AWS
	"""
	def __init__(self):
		self.connected = False
		self.conn = None
		self.prefix = None
		self.tables = {}

	def set_prefix(self, prefix):
		"""
		Assign the table prefix
		"""
		self.prefix = prefix

	def connect(self, region_name, access_key, secret_key):
		"""
		Initialise connection to AWS
		"""
		region = None
		for r in regions():
			if r.name == region_name:
				region = r
				break

		if not region:
			raise Exception('Invalid DynamoDB region specified')

		try:
			self.conn = DynamoDBConnection(region=region, 
						aws_access_key_id=access_key,
						aws_secret_access_key=secret_key)

			self.connected = True
		except Exception as e:
			raise Exception('Failed to connect to AWS')

	def _get_table(self, table_name):
		"""
		Lazy connection of tables

		Internal use only
		"""
		if not self.connected:
			raise Exception('Attempting to retrieve table but no connection available')
		if not self.prefix:
			raise Exception('Attempting to retrieve table prior to prefix assignment')

		table = self.tables.get(table_name, None)
		if not table:
			table = Table('_'.join([self.prefix, table_name]), connection=self.conn)
			self.tables[table_name] = table
		return table

	def _save_to_table(self, table_name, item):
		"""
		Save an item to the specified table

		Internal use only
		"""
		try:
			return self._get_table(table_name).put_item(item)

		except Exception as e:
			raise SaveError(e.message)


	def _validate_data(self, required_data, data):
		"""Validates that the required fields are present"""

		# Should have only this data and no more or less
		if len(data) != len(required_data):
			raise ValidationError('Incorrect data length supplied')

		# Verify names and types are correct
		for field, field_type in required_data:
			if not field in data:
				raise ValidationError('Invalid data supplied')
			else:
				if field_type == 'N' and not isinstance(data[field], int):
					raise ValidationError('Invalid data type supplied')


	def _get_latest_org_details(self, org_id):
		"""Retrieves latest details for the specified organisation"""
		try:
			return list(self._get_table('Org').query(
					org_id__eq = org_id,
					reverse = False,		# By default brings latest, set to True to get earliest
					limit = 1))				# Sets the retrieval count		

		except Exception as e:
			print e
			raise ValidationError('Error retrieving organisation details')

	def _validate_org(self, org_id):
		"""Validates existence of the org, and if it is active"""
		org_info = self._get_latest_org_details(org_id)
		if not org_info:
			# Org doesnt exist
			raise ValidationError('Specified organisation does not exist')

		# Have a record; check if the organisation is active
		return True if org_info['status'] == 1 else False


	def _validate_register_data(self, data):
		"""Validates that all required fields are present"""

		# This is the set of data to be saved
		REQUIRED_FIELDS = [('name', 'S'), ('contact', 'S'), ('website', 'S')]

		# Validate structure
		self._validate_data(REQUIRED_FIELDS, data)


	def _validate_save_data(self, org_id, service_id, data):
		"""Validates that the required fields are present"""

		# This is the set of data to be saved
		REQUIRED_FIELDS = [('timestamp', 'N'), ('obo_id', 'S'), ('actor_id', 'S')]

		# Validate structure
		self._validate_data(REQUIRED_FIELDS, data)

		# Validate that the organistion exists and is active
		if not self._validate_org(org_id):
			raise ValidationError('Invalid organisation supplied')


	def register_org(self, org_id, data):
		"""
		Save the organisation information against the specified 
		"""

		# Validate the supplied data is complete
		self._validate_register_data(data)

		if not org_id:
			# New organisation 
			# TODO: check prior existence before creating, but for now just create
			org_id = uuid()

		item = {}
		item['org_id'] = str(org_id)
		item['timestamp'] = get_tm()
		item['name'] = data['name']
		item['contact'] = data['contact']
		item['website'] = data['website']
		item['status'] = 1 						# Mark as active

		self._save_to_table('Org', item)

		# Return original data plus identifier
		data['id'] = org_id
		return data


	def unregister_org(self, org_id):
		"""
		Unregister the organisation information against the specified 
		"""

		if not org_id:
			# New organisation 
			raise ValidationError('No organisation identifier supplied for unregistration')

		item = {}
		item['org_id'] = org_id
		item['timestamp'] = get_tm()
		item['status'] = 0 						# Mark as inactive

		self._save_to_table('Org', item)

		# Return identifier
		return {'id':org_id}


	def save_data(self, org_id, service_id, data):
		"""
		Save the audit information supplied by the specified org/service pair
		"""

		# Ensure we can use the data
		self._validate_save_data(org_id, service_id, data)

		# Create item
		item = {}
		item['service-org_hash'] = create_hash(service_id, org_id)
		item['org_user_hash'] = create_hash(org_id, data['obo_id'])
		item['timestamp'] = data['timestamp']
		item['obo_id'] = data['obo_id']
		item['actor_id'] = data['actor_id']

		self._save_to_table('Audit', item)

