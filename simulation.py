import urllib.request
from io import StringIO
import csv
import argparse
import datetime
from pythonds.basic.queue import Queue


class Server:
    def __init__(self):

        self.currentTask = None
        self.timeRemaining = 0

    def tick(self):
        if self.currentTask != None:
            self.timeRemaining = self.timeRemaining - 1
            if self.timeRemaining <= 0:
                self.currentTask = None

    def busy(self):
        if self.currentTask != None:
            return True
        else:
            return False

    def startNext(self, newtask):
        self.currentTask = newtask
        self.timeRemaining = newtask.getTime()


class Request:
    def __init__(self, data):
        self.timestamp = int(data[0])
        self.timetaken = int(data[2])

    def getStamp(self):
        return self.timestamp

    def getPages(self):
        return self.pages

    def getTime(self):
        return self.timetaken

    def waitTime(self, currenttime):
        return currenttime - self.timestamp


def main():

    commandParser = argparse.ArgumentParser(description="Send a ­­url parameter to the script")
    commandParser.add_argument("--file", type=str, help="Link to the csv file")
    commandParser.add_argument("--servers", type=int, help="Link to the csv file")
    args = commandParser.parse_args()
    if not args.file:
        exit()
    if not args.servers:
        simulateOneServer(args.file)
    else:
        simulateManyServers(args.file, args.servers)


def simulateOneServer(file):

    content = urllib.request.urlopen(file).read().decode("ascii", "ignore")
    data = StringIO(content)
    csv_reader = csv.reader(data, delimiter=',')

    dataList = []

    for line in csv_reader:
        dataList.append(line)

    requestQueue = Queue()
    waitingtimes = []
    server = Server()
    for i in dataList:
        request = Request(i)
        requestQueue.enqueue(request)
        if (not requestQueue.isEmpty()) and (not server.busy()):
            nexttask = requestQueue.dequeue()
            waitingtimes.append(nexttask.waitTime(int(i[0])))
            server.startNext(nexttask)
        server.tick()
        averageWait = sum(waitingtimes) / len(waitingtimes)
        print("Average Wait %6.2f secs %3d tasks remaining." % (
        averageWait, requestQueue.size()))

    print("Average latency is {} seconds".format(averageWait))
    return averageWait


def simulateManyServers(file, noOfServers):
    content = urllib.request.urlopen(file).read().decode("ascii", "ignore")
    data = StringIO(content)
    csv_reader = csv.reader(data, delimiter=',')

    dataList = []
    for line in csv_reader:
        dataList.append(line)

    requestQueue = Queue()
    waitingtimes = []
    servers = [Server() for a in range(noOfServers)]

    for i in dataList:
        request = Request(i)
        requestQueue.enqueue(request)

        for server in servers:
            if (not server.busy()) and (
            not requestQueue.isEmpty()):
                nexttask = requestQueue.dequeue()
                waitingtimes.append(nexttask.waitTime(int(i[0])))
                server.startNext(nexttask)
            server.tick()
        averageWait = sum(waitingtimes) / len(waitingtimes)
        print("Average Wait %6.2f secs %3d tasks remaining." % (averageWait, requestQueue.size()))

    print("Average latency is {} seconds".format(averageWait))
    return (averageWait)

if __name__ == "__main__":
    main()