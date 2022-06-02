import requests

class SeqServer:
    END_POINT = "api/events/raw?clef"
    HEADERS = {'Content-Type': 'application/vnd.serilog.clef'}

    def __init__(self, serverUrl):
        self.setServerUrl(serverUrl)
        self.setUrl()
    
    def setServerUrl(self, serverUrl):
        self.serverUrl = serverUrl

    def setUrl(self):
        self.Url = self.serverUrl + self.END_POINT

    def sendRequest(self, payloadStr):
        r = requests.post(url= self.Url, data= payloadStr, headers= self.HEADERS)
        print("Request Status Code: {}\n\n".format(r.status_code))