#!/usr/bin/python3-4

from multiprocessing import Process
from subprocess import call
from shlex import split
import argparse

argparser = argparse.ArgumentParser(description="Send a command either to all pis or all but this one. Make sure to place the command in quotes")
#find_lim_group = argparser.add_mutually_exclusive_group()

argparser.add_argument('command', help="the command to be executed on the pis")
argparser.add_argument('-s', '--serial', action='store_true', help="execute commands in serial. default is parallel")

argparser.add_argument('-a', '--all', action='store_true', help="If this flag is used, the command will also execute on this pi")

args = argparser.parse_args()

def send_command(num, command):
	print("Executing on pi0"+str(num)+"...")
	call(['ssh', 'pi0'+str(num), command])

if __name__ == '__main__':
	print("main")
	if args.serial:
		for i in range(2,5):
			print("Executing on pi0"+str(i)+"...")
			call(['ssh', 'pi0'+str(i), command])
		if args.all:
			print("Executing on pi01...")
			call(split(command))
	else:
		print("parallel")
		numProcs = 3
		if args.all:
			numProcs = 4
		for x in range(2,5):
			#r, w = Pipe(duplex=False)
			#readers.append(r)
			p = Process(target=send_command, args=(x, args.command,))
			p.start()
			#w.close()
		if args.all:
			print("Executing on pi01...")
			call(split(args.command))
		#while readers:
		#	for r in wait(readers):
		#		try:
		#			msg = r.recv()
		#		except EOFError:
		#			readers.remove(r)
		#		else:
		#			print(msg)
