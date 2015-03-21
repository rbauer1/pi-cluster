#!/usr/bin/python3

from subprocess import call, STDOUT, CalledProcessError, check_output
import argparse

argparser = argparse.ArgumentParser(description="Send a file or files to all other pis using scp. If the specified directory doesn't exist on the other pis, it will be created")

argparser.add_argument('path', help="the path of the file(s) to be sent the pis")
argparser.add_argument('-d', '--destination', help="Use if destination path is different from source path. If not specified, source path will be used")

args = argparser.parse_args()

def sendFiles():
	if args.destination is None:
		args.destination = args.path
	try:
		for i in range(2,5):
			check_output(['scp', args.path, 'pi0'+str(i) + ':' + args.destination], stderr=STDOUT)
		print("Finished")	
	except CalledProcessError as err:
		error = err.output.decode("utf-8")
		print(error)
		if args.destination in error:
			for i in range(2,5):
                        	call(['ssh', 'pi0'+str(i), 'mkdir -p '+'/'.join(args.destination.split("/")[:-1])])
			sendFiles()

sendFiles()
