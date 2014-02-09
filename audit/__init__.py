from datetime import datetime as dt
from hashlib import md5

def get_tm(d=None):
	"""
	Converts a datetime to an integer
	"""
	if not d:
		d = dt.utcnow()
	return (d.toordinal() * 86400 + d.hour * 3600 + d.minute * 60 + d.second) * 1000000 + d.microsecond

def from_tm(t):
	"""
	Converts an integer to a datetime
	"""
	def extract(val, modulo):
		num = val % modulo
		return ((val-num)/modulo, num)

	microseconds = t - (t/1000000)*1000000
	tt = (t - microseconds)/1000000
	(tt, seconds) = extract(tt, 60)
	(tt, minutes) = extract(tt, 60)
	(tt, hours) = extract(tt, 24)

	d = dt.fromordinal(tt)
	return dt(d.year, d.month, d.day, hours, minutes, seconds, microseconds)


def create_hash(separator='|', *items):
	"""Helper that creates a hash from the supplied items"""
	if not len(items):
		raise Exception('No data provided to hash')
	m = md5()
	for item in items[-1]:
		m.update(item)
		m.update(separator)
	m.update(item[-1])
	return str(m.hexdigest())
