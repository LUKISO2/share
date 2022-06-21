import gzip
import sys
import json
import re
import os.path

def editMe(fileToBeOpened=None):
    # Checking for arguments
    if fileToBeOpened == None:
        if sys.argv[0] == sys.argv[-1]:
            print("No file specified!")
            sys.exit()
        else:
            fileToBeOpened = sys.argv[-1]
    finalResult = []

    if "-o" in sys.argv:
        try:
            outputPath = sys.argv[sys.argv.index("-o") + 1]
        except:
            print("ERROR - Invalid output path!")
            sys.exit()
        if not os.path.isdir(outputPath):
            print("ERROR - Output directory does not exist!")
            sys.exit()
        changeOutputPath = True
    else:
        changeOutputPath = False

    # Open the file
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

    # Decode the file
    contents = contents.decode("utf-8")
    values = contents.split("\n")
    try:
        values = values[:len(values)-1]
    except:
        print("File is empty")
        sys.exit()

    # Hasher for phone numbers
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

    # Hasher for text with at character
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

    # Telephojne number checker
    def isATelNumber(value):
        return True if re.match("^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{3})(?: *x(\d+))?\s*$", value) else False
    
    # Used for determining output path
    def getOutputPath(changeOutputPath=changeOutputPath):
        rawFileName = os.path.basename(fileToBeOpened)
        rawFileName = rawFileName.split(".")
        finalName = rawFileName[0] + "_hashed.json.gz"
        if changeOutputPath == False:
            return os.path.join(os.path.dirname(fileToBeOpened), finalName)
        elif changeOutputPath == True:
            return os.path.join(outputPath, finalName)

    # Main loop
    for value in values:
        innerValue = value.split("|")
        mainJson = json.loads(innerValue[11])

        # Head to json
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

        # Hasing in json values
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

    # Write to file
    finalFile = gzip.open(getOutputPath(), "wb")
    for item in finalResult:
        finalFile.write(item.encode() + b"\n")
    finalFile.close()

if __name__ == "__main__":
    editMe()
