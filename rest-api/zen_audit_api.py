"""
This defines the whole RESTful API that will run within flask

The AWS credentials must be able to write data to the Audit service tables in AWS DynamoDB

"""
import argparse
from audit import get_tm, create_hash
from audit.aws import Audit
from datetime import datetime as dt
from flask import Flask, abort, request, jsonify, make_response

# Provides all audit functionality
audit = Audit()

app = Flask(__name__)

@app.route('/1.0/audit/org/', methods=['GET'])
def get_org_info():
	"""
	Returns the set of organisations registered with the service, and their current status
	"""
	return make_response(jsonify({'status':200, 'error_message':''}), 200)

@app.route('/1.0/audit/org/register/', methods=['POST'])
def register_org():
	"""
	Registers an organisation and returns a unique identifier
	to be used when saving data.

	Invoking without the identifier in the header 'Audit-Identifier' will return the same
	identifier if the contents are matched.

	Invoking with the returned identifier included in the body will cause the details
	of the organisation to be updated in the service if the identifier is valid, otherwise and
	error will be returned.

	Invoking with the identifier after a DELETE will reinstate the organisation (if within 24 hours of
	the deletion request), otherwise it will create a new identifier for the same details.

	Body should contain JSON of the form:

	{
		"name":"The name of the organisation",
		"contact":"Email of the contact in the organisation",
		"website":"Website of the organisation"
	}

	A successful registration will return a status code of 200 and returns JSON of the form:

	{
		"id":"The identifier for this organisation",
		"name":"The name of the organisation",
		"contact":"Email of the contact in the organisation",
		"website":"Website of the organisation"
	}

	A failure will return the relevant status code and JSON of the form:

	{
		"status":"The status code returned by the service",
		"message":"A description of the error that occurred"
	}

	"""
	try:
		org_id = request.headers.get('Audit-Identifier', None)
		result = audit.register_org(org_id, request.get_json())
		return jsonify(result)

	except Exception as e:
		return make_response((jsonify({'status':404, 'error_message':e.message}), 404))

@app.route('/1.0/audit/org/register/', methods=['DELETE'])
def unregister_org():
	"""
	Unregisters the organisation whose identifier is in the 'Audit-Identifier' header.

	Specifing DELETE will queue the organisation and all its associated data for deletion,
	which will occur 24 hours after the request was made.

	The body of the request is ignored, but the header 'Audit-Identifier' must be present,
	containing the identifier of the organisation to be removed.

	A successful unregistration will return a status code of 200 and returns JSON of the form:

	{
		"id":"The identifier for this organisation"
	}

	A failure will return the relevant status code and JSON of the form:

	{
		"status":"The status code returned by the service",
		"message":"A description of the error that occurred"
	}

	"""
	try:
		org_id = request.headers.get('Audit-Identifier', None)
		result = audit.unregister_org(org_id)
		return jsonify(result)

	except Exception as e:
		return make_response((jsonify({'status':404, 'error_message':e.message}), 404))

@app.route('/1.0/audit/org/<org>/services/', methods=['GET'])
def get_org_services(org_id):
	"""
	Returns the set of services registered with this service for the specified org identifier
	"""
	return make_response(jsonify({'status':200, 'error_message':''}), 200)

@app.route('/1.0/audit/org/<org>/services/register/', methods=['POST'])
def register_org_service(org_id):
	"""
	Registers a service used by the organisation and returns a unique identifier
	to be used when saving data against that service.

	Repeated registrations with the same details will have no effect and will return the
	orginal identifier associated with the registered service, for the organisation.

	Supplying the service identifier in the header 'Audit-Identifier' will cause the stored details
	for the service to be updated, unless the identifier cannot be found when an error will be returned.

	The organisation represented by the identifier in the URL must be registered and
	active with the service, or an error will be returned.

	Invoking after a DELETE will reinstate the service registration, provided the 'Audit-Identifier' is
	specified in the header and the request is made within 24 hours of the unregistration request.  Otherwise
	a new record will be created with a new service identifier.

	Body of the request should contain JSON of the form:

	{
		"name":"Name of the service"
		...
	}

	A successful registration will return a status code of 200 and returns JSON of the form:

	{
		"id":"The identifier for this service, given the organisation",
		"name":"Name of the service"
		...
	}

	A failure will return the relevant status code and JSON of the form:

	{
		"status":"The status code returned by the service",
		"message":"A description of the error that occurred"
	}


	"""
	return make_response(jsonify({'status':200, 'error_message':''}), 200)

@app.route('/1.0/audit/org/<org>/services/register/', methods=['DELETE'])
def un_register_org_service(org_id):
	"""
	Unregisters the specified service from the organisation.

	Specifing DELETE will queue the service and all its associated data for deletion,
	which will occur 24 hours after the request was made.

	The body of the request should be empty, and the header 'Audit-Identifier' must be present,
	containing the identifier of the service to be removed.

	A successful unregistration will return a status code of 200 and returns JSON of the form:

	{
		"id":"The identifier for this service, given the organisation",
		"name":"Name of the service"
		...
	}

	A failure will return the relevant status code and JSON of the form:

	{
		"status":"The status code returned by the service",
		"message":"A description of the error that occurred"
	}
	"""
	return make_response(jsonify({'status':200, 'error_message':''}), 200)


@app.route('/1.0/audit/org/<org_id>/services/<service_id>/save/', methods=['POST'])
def save_audit(org_id, service_id):
	"""
	Saves the audit data against the specified organisation and service identifiers.

	Both the organisation and service identifiers must exist, and be active, otherwise
	an error will be generated.

	The structure of the supplied data must also be complete for the save to occur.  

	Saves are not idempotent, so that repeated calls will add additional records in the service.
	"""

	try:
		tm_start = dt.utcnow()
		save_status = audit.save_data(org_id, service_id, request.get_json())
		tm_end = dt.utcnow()

		resp_data = {
				"status": "saved" if save_status else "failed",
				"total_time": get_tm(tm_end) - get_tm(tm_start)
			}

		return jsonify(resp_data)

	except Exception as e:
		return make_response((jsonify({'status':404, 'error_message':e.message}), 404))


@app.route('/')
@app.route('/<path:varargs>')
def bad_routing_start(varargs = None):
	abort(404)

if __name__ == "__main__":

	# Process arguments
	parser = argparse.ArgumentParser(description='This runs the flask based web-server providing the API')
	parser.add_argument('-d','--debug', help='Run in debug', default=False, required=False)
	parser.add_argument('-r','--region', help='DynamoDB region', required=True)
	parser.add_argument('-k','--access_key', help='AWS Access Key', required=True)
	parser.add_argument('-s','--secret_key', help='AWS Secret Key', required=True)
	parser.add_argument('-p','--prefix', help='The table prefix in DynamoDB', required=True)
	args = parser.parse_args()

	# Let's connect and make ourselves available
	audit.set_prefix(args.prefix)
	audit.connect(args.region, args.access_key, args.secret_key)

	# Start flask
	app.run(debug=arg.debug)
