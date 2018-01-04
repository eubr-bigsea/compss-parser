def read_files(logpath):
	import os
	return [s for s in os.listdir(logpath) if not s.startswith('.DS_Store') and not os.path.isdir(logpath + "/" + s)]
	#return [s for s in os.listdir(logpath) if s.startswith('app') or s.startswith('eventLogs')]

def write_file(filename, data):
	text_file = open(filename, "w")
	text_file.write(data)
	text_file.close()

def str_to_datetime(date_str):
	from datetime import datetime
	return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S,%f')

def sub_str_datetimes(end_date, start_date):
	start = str_to_datetime(start_date)
	end = str_to_datetime(end_date)

	delta = end - start
	return delta.total_seconds()*1000.0