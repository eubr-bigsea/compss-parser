import os, sys, json, getopt, time
from parser import execute

def main(argv):
	start_time = time.time()

	# Getting params
	try:
	  	opts, args = getopt.getopt(argv,"l:o:")
	except getopt.GetoptError:
	  	print 'python run.py -l <logdir> -o <output>'
	  	sys.exit(2)

  	outputdir = os.path.dirname(os.path.realpath(__file__)) + '/output';

	# iterating over the params
	for opt, value in opts:
		if opt == "-l":
			logdir = os.path.realpath(value)
		if opt == "-o":
			outputdir = value

	# executing model
	execute(logdir, outputdir)

	elapsed_time = time.time() - start_time
	print "Elapsed time: %f ms" % (elapsed_time*1000)
	sys.exit()


if __name__ == "__main__":
   main(sys.argv[1:])
