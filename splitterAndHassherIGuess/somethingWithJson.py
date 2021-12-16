import gzip
import sys
import json
import re
import os.path

def editMe(fileToBeOpened=None):
    try:
        fileToBeOpened = sys.argv[1]
    except:
        if fileToBeOpened is None:
            print("No file specified")
            sys.exit()
    finalResult = []
    try:
        if os.path.isfile(fileToBeOpened):
            gunzip = gzip.open(fileToBeOpened, "rb")
            contents = gunzip.read()
            gunzip.close()
        else:
            print("File does not exist")
            sys.exit()
    except:
        print("File isn't gzipped")
        sys.exit()

    contents = contents.decode("utf-8")
    values = contents.split("\n")
    try:
        values = values[:len(values)-1]
    except:
        print("File is empty")
        sys.exit()

    def hasher(value):
        hashing = []
        strValue = str(value)
        toHash = strValue[len(strValue)-6:]
        leaveBehind = strValue[:len(strValue)-6]
        for letter in toHash:
            hashing.append(letter)
        hashing.insert(1, hashing.pop(0))
        hashing.insert(6, hashing.pop(2))
        hashing.insert(6, hashing.pop(2))

        hashed = "".join(hashing)
        final = leaveBehind + hashed
        return final

    def atHasher(value):
        preHash = value.split("@")
        if len(preHash) == 1:
            return value
        toHash = preHash[0]
        hashing = []
        for letter in toHash:
            hashing.append("x")
        hashed = "".join(hashing)
        final = hashed + "@" + preHash[1]
        return final

    def isATelNumber(value):
        if re.match("^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{3})(?: *x(\d+))?\s*$", value):
            return True
        else:
            return False

    for value in values:
        innerValue = value.split("|")
        mainJson = json.loads(innerValue[11])

        if innerValue[0] != "":
            mainJson["ne_type"] = innerValue[0]
        if innerValue[1] != "":
            mainJson["ne_id"] = innerValue[1]
        if innerValue[2] != "":
            if isATelNumber(innerValue[2]) == True:
                mainJson["a_party_msisdn"] = hasher(innerValue[2])
            else:
                mainJson["a_party_msisdn"] = innerValue[2]
        if innerValue[3] != "":
            try:
                innerValue[3] = int(innerValue[3])
                if len(str(innerValue[3])) == 15 or len(str(innerValue[3])) == 16:
                    mainJson["a_party_imsi"] = hasher(innerValue[3])
                else:
                    mainJson["a_party_imsi"] = innerValue[3]
            except:
                mainJson["a_party_imsi"] = innerValue[3]
        if innerValue[4] != "":
            mainJson["a_party_location"] = innerValue[4]
        if innerValue[5] != "":
            mainJson["b_party_msisdn"] = innerValue[5]
        if innerValue[6] != "":
            mainJson["b_party_location"] = innerValue[6]
        if innerValue[7] != "":
            mainJson["timestamp"] = innerValue[7]
        if innerValue[8] != "":
            mainJson["timestamp_offset"] = innerValue[8]
        if innerValue[9] != "":
            mainJson["c_party_msisdn"] = innerValue[9]
        if innerValue[10] != "":
            mainJson["c_party_location"] = innerValue[10]

        try:
            if isATelNumber(mainJson["servedMSISDN"]["number"]) == True:
                mainJson["servedMSISDN"]["number"] = hasher(mainJson["servedMSISDN"]["number"])
        except:
            pass
        try:
            x = int(mainJson["servedIMSI"]["IMSI"])
            if len(str(mainJson["servedIMSI"]["IMSI"])) == 15 or len(str(mainJson["servedIMSI"]["IMSI"])) == 16:
                mainJson["servedIMSI"]["IMSI"] = hasher(mainJson["servedIMSI"]["IMSI"])
        except:
            pass
        try:
            mainJson["servedIMEISV"]["IMEI"] = hasher(mainJson["servedIMEISV"]["IMEI"])
        except:
            pass
        try:
            mainJson["servedMNNAI"]["subscriptionIDData"] = atHasher(mainJson["servedMNNAI"]["subscriptionIDData"])
        except:
            pass

        finalResult.append(json.dumps(mainJson))

    rawFileName = os.path.basename(fileToBeOpened)
    rawFileName = rawFileName.split(".")
    finalName = rawFileName[0] + "_hashed.json"
    finalFile = open(os.path.abspath(fileToBeOpened) + finalName, "w")
    for item in finalResult:
        finalFile.write(item + "\n")

if __name__ == "__main__":
    editMe("//nasjh/Shared/DEVELOP/bd_epc_pgw.load.gz")
