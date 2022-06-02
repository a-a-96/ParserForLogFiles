import sh
import json
from itertools import islice

class LogsFileParser:
    
    def __init__(self, filePath, timeKey, server):
        self.setFilePath(filePath)
        self.setTimeKey(timeKey)
        self.setServer(server)

    def setServer(self, server):
        self.server = server
    
    def setFilePath(self, filePath):
        self.filePath = filePath
    
    def setTimeKey(self, timeKey):
        self.timeKeys = self.__getKeyList(timeKey)

    def tailLogsFile(self):
        fileLines = self.__tail_F(self.filePath)
        for line in  fileLines:
            payloadStr = self.__generatePayloadStr(line, self.timeKeys)
            self.server.sendRequest(payloadStr)

    def batchLogsFile(self, batchSize, skipSize):
        numLines = sum(1 for _ in open(self.filePath))
        if skipSize:
            endingLine = numLines - skipSize
        else:
            endingLine = numLines
        startingLine = endingLine - batchSize

        print("BatchSize: {} \nSkipSize: {} \nStartingLine: {} \nEndingLine: {}".format(batchSize, skipSize, startingLine, endingLine))

        with open(self.filePath) as f:
            lines = islice(f, startingLine, endingLine)
            for line in lines:
                payloadStr = self.__generatePayloadStr(line, self.timeKeys)
                self.server.sendRequest(payloadStr)
        
    
    def __tail_F(self, file):
        while True:
            try:
                for line in sh.tail("-f", file, _iter=True):
                    yield line
            except sh.ErrorReturnCode_1:
                yield None

    def __getTimeValue(self, timeKeysList, jsonObject):
        for i in timeKeysList:
            jsonObject = jsonObject[i]
        return jsonObject

    def __setTimeValue(self, payloadStr, timeValue):
        payloadStr += "\"@t\":"+"\""+str(timeValue)+"\""+","
        return payloadStr

    def __generatePayloadStr(self, line, timeKeys):
        payloadStr = "{"
        payloadJson = json.loads(line)

        timeValue = self.__getTimeValue(timeKeys, payloadJson)
        payloadStr = self.__setTimeValue(payloadStr, timeValue)

        jsondump = self.__getJsonDump(line)
        payloadStr += "\"@m\":"+ jsondump

        payloadStr = self.__addDictionaryToPayloadString(payloadStr, payloadJson)
        payloadStr += "}\n"

        print("=========================================================\n")
        print("Adding log: {}".format(line))

        return payloadStr

    def __getKeyList(self, keyString):
        return [str(x) for x in keyString.split(".")]

    def __addDictionaryToPayloadString(self, payloadStr, dictionaryObject):
        for key, value in dictionaryObject.items():
            payloadStr = self.__addKeyValuePairToPayloadString(payloadStr, key, value)

            valueType = type(value)
            if (valueType == dict):
                payloadStr = self.__addDictionaryToPayloadString(payloadStr, value)
            elif (valueType == list):
                payloadStr = self.__addListOfDictionaryToPayloadString(payloadStr, value)
        return payloadStr

    def __addListOfDictionaryToPayloadString(self, payloadStr, listObject):
        for i in listObject:
            if (type(i) == dict):
                payloadStr = self.__addDictionaryToPayloadString(payloadStr, i)
        return payloadStr

    def __addKeyValuePairToPayloadString(self, payloadStr, key, value):
        payloadStr += ", \"" + str(key) + "\": " + self.__getJsonDump(value)
        return payloadStr

    def __getJsonDump(self, jsonObject):
        return json.dumps(jsonObject)