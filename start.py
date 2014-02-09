import argparse
from rest_api.zen_audit_api import audit
from install.audit_install_db import create_tables
from uuid import uuid4 as uuid

if __name__ == "__main__":

    # Process arguments
    parser = argparse.ArgumentParser(description='This installs the DynamoDB tables required for the Audit servce')
    parser.add_argument('-r','--region', help='DynamoDB region', required=True)
    parser.add_argument('-k','--access_key', help='AWS Access Key', required=True)
    parser.add_argument('-s','--secret_key', help='AWS Secret Key', required=True)
    args = parser.parse_args()

    # New prefix each start
    prefix = str(uuid())

    ret = create_tables(args.region, args.access_key, args.secret_key, prefix)

    audit.set_prefix(prefix)
    audit.connect(args.region, args.access_key, args.secret_key)


