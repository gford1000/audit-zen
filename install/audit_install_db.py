"""
This script creates an environment of the audit-zen DynamoDB tables, which comprise:

	Audit table - this holds the details of the action that was performed

The tables will have an optional prefix that allows multiple installs side by side.

It is expected that the AWS account used has sufficient access to create the tables, and associated alarms

"""

from boto.dynamodb2 import regions as db2_regions
import boto.dynamodb2.layer1 as db2

class CreateAuditError(Exception):
	"""Allows identification of creation specific errors"""
	pass

def create_tables(region_name, access_key, secret_key, prefix=None):
	"""
	Create the tables for the audit service, in the region
	"""
	def create_arg(set_tags, attr_def):
		"""Helper to build JSON for attributes"""
		attrs = []
		for attr_name, attr_type in attr_def:
			attr_set = {}
			attr_set[set_tags[0]] = attr_name
			attr_set[set_tags[1]] = attr_type
			attrs.append(attr_set)
		return attrs

	def create_throughput((read, write)):
		"""Helper to build JSON for provisioning"""
		throughput = {}
		throughput['ReadCapacityUnits'] = read
		throughput['WriteCapacityUnits'] = write
		return throughput

	def create_idx(idx_name, schema, projection, throughput):
		"""Helper for index creation"""
		idx = {}
		idx['IndexName'] = idx_name
		idx['KeySchema'] = create_arg(['AttributeName', 'KeyType'], schema)
		idx['Projection'] = projection
		idx['ProvisionedThroughput'] = create_throughput(throughput)
		return idx

	def create_table(conn, prefix, table_name, attr_def, key_def, provisioning, idx_set=[]):
		"""Creates the table using the specified connection"""
		gsi = []
		for idx_def in idx_set:
			gsi.append(create_idx(
				idx_def['name'],
				idx_def['schema'],
				{'ProjectionType':'KEYS_ONLY'},
				idx_def['provisioning']
				))
	
		full_name = create_full_table_name(prefix, table_name)

		if len(gsi):
			return (full_name, conn.create_table(
						table_name=full_name,
						attribute_definitions= create_arg(['AttributeName', 'AttributeType'], attr_def),
						key_schema=create_arg(['AttributeName', 'KeyType'], key_def),
						global_secondary_indexes=gsi,
						provisioned_throughput = create_throughput(provisioning)))
		else:
			return (full_name, conn.create_table(
						table_name=full_name,
						attribute_definitions= create_arg(['AttributeName', 'AttributeType'], attr_def),
						key_schema=create_arg(['AttributeName', 'KeyType'], key_def),
						provisioned_throughput = create_throughput(provisioning)))			

	def create_full_table_name(prefix, table_name):
		"""Helper to create table name"""
		return table_name if not prefix else '_'.join((prefix, table_name))

	def create_audit(conn, prefix, provisioning):
		"""Creates the Audit table"""
		attr_def = [('service-org_hash', 'S'), ('org-user_hash', 'S'), ('timestamp', 'N')]
		key_def = [('service-org_hash', 'HASH'), ('timestamp', 'RANGE')]
		idx1_schema = [('org-user_hash', 'HASH'), ('timestamp', 'RANGE')]
		idx1 = {'name':'org-user', 'schema':idx1_schema, 'provisioning':provisioning}

		return create_table(conn, prefix, 'Audit', attr_def, key_def, provisioning, [idx1])

	def create_org(conn, prefix, provisioning):
		"""Creates the Org table"""
		attr_def = [('org_id', 'S'), ('timestamp', 'N')]
		key_def = [('org_id', 'HASH'), ('timestamp', 'RANGE')]
		return create_table(conn, prefix, 'Org', attr_def, key_def, provisioning)

	def create_org_services(conn, prefix, provisioning):
		"""Creates the OrgService table"""
		attr_def = [('org-service_id', 'S'), ('timestamp', 'N')]
		key_def = [('org-service_id', 'HASH'), ('timestamp', 'RANGE')]
		return create_table(conn, prefix, 'OrgService', attr_def, key_def, provisioning)

	def _regions():
		"""Generator for returning dynamodb2 regions"""
		for region in db2_regions():
			yield region.name

	# Validate region_name
	if region_name not in _regions():
		raise CreateAuditError('{} not a known AWS region'.format(region_name))

	# Create connection
	region = None
	for r in db2_regions():
		if r.name == region_name:
			region = r
			break

	try:
		conn = db2.DynamoDBConnection(region=region, 
					aws_access_key_id=access_key,
					aws_secret_access_key=secret_key)
	except Exception as e:
		raise CreateAuditError('Failed to connect to AWS')

	resp = {}
	try:
		# Create Audit table - tuple is (ReadCapacity, WriteCapacity)
		status = create_audit(conn, prefix, (2,5))
		resp['Audit'] = {'name':status[0], 'status':status[1]}

		# Create Org table
		status = create_org(conn, prefix, (1,1))
		resp['Org'] = {'name':status[0], 'status':status[1]}

		# Create OrgService table
		status = create_org_services(conn, prefix, (1,1))
		resp['OrgService'] = {'name':status[0], 'status':status[1]}

	except Exception as e:
		raise CreateAuditError('Failed to create Audit table: {}'.format(e))

	# Return the outcome of the request
	return resp

if __name__ == "__main__":
	"""Creates tables using gford1000-Dev credentials"""
	from uuid import uuid4 as uuid
	region = 'ap-southeast-1'
	ret = create_tables(region, 'AKIAIXFYGCD7RW76NOPA', 'jZy+Hh90NWc0PfqHW1MsA93m/1+5+kY6p80PFeu6', str(uuid()))

	print ret
