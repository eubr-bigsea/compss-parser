import json, sys, re, os, math, time
from util import write_file, sub_str_datetimes, read_files

# read log line by line as json
def read_log(logpath):
	return [line.replace("\r", "").replace("\n", "") for line in tuple(open(logpath, 'r'))]

# parse DAG
def parse_DAG(logPath):
	events = read_log(logPath)

	# defining a pattern to match the event date
	# format: (2017-02-20 12:42:32,066)
	patternDatetime = "(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}),(\d{3})"
	pd = re.compile(patternDatetime)

	# defining a pattern to match the job and task id
	patternTask = "New Job (\d*) \(Task: (\d*)\)"
	pt = re.compile(patternTask)

	# definind a pattern to match task id on @endTask event
	patternTaskEnd = "Notification received for task (\d*) with end status FINISHED"
	pte = re.compile(patternTaskEnd)
	
	app = {}
	stages = []
	tasks = {}

	first_task_datetime = 0
	first_task = 0
	end_task = 0
	end_task_datetime = 0
	
	# iterating over all events to fetch starting and ending times of tasks
	for log in events:

		# finding a start task event - task line
		if "@doSubmit" in log and "Task:" in log:
			m = pd.search(log)
			datetime = m.group(0)

			n = pt.search(log)
			job_id = int(n.group(1))
			task_id = int(n.group(2))
			tasks[task_id] = {"start": datetime, "end": 0}

		# finding an end task event
		if "@endTask" in log and "status FINISHED" in log:
			m = pd.search(log)
			datetime = m.group(0)
			# timestamp = datetime_to_unix_timestamp(datetime)

			m = pte.search(log)
			task_id = int(m.group(1))

			tasks[task_id]["end"] = datetime

		# synchronizing, 
		# however it's needed to check if there is more than one synchronization after the current stage
		if "@waitForTask" in log and "End of waited task for data":
			if tasks:
				stages.append(tasks)
				tasks = {}

		if "@init" in log:
			m = pd.search(log)
			start = m.group(0)

		if "@noMoreTasks" in log:
			m = pd.search(log)
			end = m.group(0)

	return sub_str_datetimes(end, start), stages

def parse_logs(logpath):
	files = read_files(logpath)

	dags = {}
	for filename in files:
		appTime, app = parse_DAG(logpath + "/" + filename)

		ahash = len(app)
		if not dags.has_key(ahash):
			dags[ahash] = []

		dags[ahash].append((appTime, app))

	return dags

def mean_dag(dags):
	meanDags = {}
	for ahash in dags:
		i = 0
		k = len(dags[ahash])
		meanAppTime = 0

		if not meanDags.has_key(ahash):
			meanDags[ahash] = []

		for appTime, app in dags[ahash]:
			meanAppTime += appTime
			i+=1
			# print appTime
			for idx, stage in enumerate(app):

				if len(meanDags[ahash]) == idx:
					meanDags[ahash].append(0)

				meanDags[ahash][idx] += stage["duration"]

				if k == i:
					meanDags[ahash][idx] /= k

			if k == i:
				meanAppTime/=k

	return meanAppTime, meanDags			



def execute(logdir, outputdir):
	dags = parse_logs(logdir)
	for ahash in dags:
		i = 0
		for appTime, app in dags[ahash]:
			summ = 0
			for idx, stage in enumerate(app):
				data = ""
				for task_id in stage:
					duration = sub_str_datetimes(stage[task_id]["end"], stage[task_id]["start"])
					data += "%f\n" % duration
				write_file(outputdir + "/%d-%d.txt"% (i, idx), data)
			i+=1