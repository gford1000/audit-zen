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

def create_tables(region_name, access_key, secret_key, provisioning, prefix=None):
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

	def create_full_table_name(prefix, table_name):
		"""Helper to create table name"""
		return table_name if not prefix else '_'.join((prefix, table_name))

	def _regions():
		"""Generator for returning dynamodb2 regions"""
		for region in db2_regions():
			yield region.name

	# Validate region_name
	if region_name not in _regions():
		raise CreateAuditError('{} not a known AWS region'.format(region_name))

	# Build names
	audit_name = create_full_table_name(prefix, 'Audit')
	ops_name = create_full_table_name(prefix, 'Ops')

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

	try:
		# Create table
		key_def = [('service-org_hash', 'HASH'), ('timestamp', 'RANGE')]
		idx1_key_def = [('org-user_hash', 'HASH'), ('timestamp', 'RANGE')]
		attr_def = [('service-org_hash', 'S'), ('org-user_hash', 'S'), ('timestamp', 'N')]
		result = conn.create_table(
					table_name=audit_name,
					attribute_definitions= create_arg(['AttributeName', 'AttributeType'], attr_def),
					key_schema=create_arg(['AttributeName', 'KeyType'], key_def),
					global_secondary_indexes=[
						create_idx('org-user',
							idx1_key_def,
							{'ProjectionType':'KEYS_ONLY'},
							provisioning)
					],
					provisioned_throughput = create_throughput(provisioning))
	except Exception as e:
		raise CreateAuditError('Failed to create Audit table')

	# Return the names of the tables created
	return {'Audit':{'name':audit_name, 'status':result}}

if __name__ == "__main__":
	"""Creates tables using gford1000-Dev credentials"""
	from uuid import uuid4 as uuid
	region = 'ap-southeast-1'
	ret = create_tables(region, 'AKIAIXFYGCD7RW76NOPA', 'jZy+Hh90NWc0PfqHW1MsA93m/1+5+kY6p80PFeu6', (2,5), str(uuid()))

	print ret
