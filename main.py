import sys
import argparse
import traceback
from SeqServer import SeqServer
from LogsFileParser import LogsFileParser


if __name__ == '__main__':
    try:
        commandLineParser = argparse.ArgumentParser()
        commandLineParser.add_argument('--file', type=str, required=True, help = 'The path to the file. The file is expected to have a json object on each line.')
        commandLineParser.add_argument('--timeKey', type=str, required=True, help = 'Time Key - The key for time value.')
        commandLineParser.add_argument('--batchSize', type=int, help = 'Batch Size - The number of tailing records to be sent to the monitoring server.')
        commandLineParser.add_argument('--skipSize', type=int, help = 'Skip Size - The number of records to be skipped from the tail of the logs file.')
        commandLineParser.add_argument('--readTail', type=str, required=True, help = 'Read Tail - A boolean that specifies whether to read the file from tail or in batches.')

        commandLineArgs = commandLineParser.parse_args()
        filePath = commandLineArgs.file
        timeKey = commandLineArgs.timeKey
        batchSize = commandLineArgs.batchSize
        skipSize = commandLineArgs.skipSize
        readTail = commandLineArgs.readTail
        payloadStr = ""

        print("\n\nFilePath: {}\nTimeKey: {}\nReadTail: {}\n".format(filePath, timeKey, readTail))

        seqServer = SeqServer("http://localhost:5341/")
        logsFileParer = LogsFileParser(filePath, timeKey, seqServer)

        if (readTail.lower() == 'true'):
            logsFileParer.tailLogsFile()

        else:
            logsFileParer.batchLogsFile(batchSize, skipSize)

    except:
        print("\n=========================================================\n")
        print("\nException:")
        traceback.print_exception(*sys.exc_info())
        print("\nExiting script")
        sys.stdout.flush()
        sys.exit()