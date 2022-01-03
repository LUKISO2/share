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
    except Exception as e:
        print(f"File isn't gzipped, error: {e}")
        sys.exit()

    # Decode the file
    contents = contents.decode("utf-8")
    values = contents.split("\n")
    try:
        values = values[:len(values)-1] if values[-1] == "" else values
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

    def chasher(value):
        strValue = str(value)
        if re.match("^\d+$", strValue[len(strValue)-6:]):
            return hasher(strValue)
        else:
            return value
    # Telephone checker
    def thasher(value):
        if re.match("^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{3})(?: *x(\d+))?\s*$", value):
            return hasher(value)
        else:
            return value
    
    # Used for determining output path
    def getOutputPath(changeOutputPath=changeOutputPath):
        rawFileName = os.path.basename(fileToBeOpened)
        rawFileName = rawFileName.split(".")
        finalName = rawFileName[0] + "_hashed.json.gz"
        if changeOutputPath == False:
            return os.path.join(os.path.dirname(fileToBeOpened), finalName)
        elif changeOutputPath == True:
            return os.path.join(outputPath, finalName)

    def getPrefix(number):
        prefixes = {'01': 'Moc', '02': 'Mtc', '03': 'Forw', '08': 'Smmo', '09': 'Smmt', '11': 'Poc', '12': 'Ptc', '13': 'Pbxo', '14': 'Pbxt', '17': 'Uca', '24': 'Coc', '25': 'Ctc', '26': 'In4', '36': 'Insu'}
        try:
            return prefixes[number]
        except:
            print("Unknown prefix! add it!")
            print("Prefix number %s" % number)
            return False

    # Main loop
    for value in values:
        innerValue = value.split("|")
        mainJson = json.loads(innerValue[11])
        prefix = getPrefix(mainJson["record_type"])
        if prefix == False:
            print(mainJson)
            continue

        # Head to json
        if innerValue[0] != "":
            mainJson["ne_type"] = innerValue[0]
        if innerValue[1] != "":
            mainJson["ne_id"] = innerValue[1]
        if innerValue[2] != "":
            mainJson["a_party_msisdn"] = thasher(innerValue[2])
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
            mainJson["b_party_msisdn"] = thasher(innerValue[5])
        if innerValue[6] != "":
            mainJson["b_party_location"] = innerValue[6]
        if innerValue[7] != "":
            mainJson["timestamp"] = innerValue[7]
        if innerValue[8] != "":
            mainJson["timestamp_offset"] = innerValue[8]
        if innerValue[9] != "":
            mainJson["c_party_msisdn"] = thasher(innerValue[9])
        if innerValue[10] != "":
            mainJson["c_party_location"] = innerValue[10]

        # Hasing in json values
        try:
            mainJson[f"{prefix}_called_imsi"] = hasher(mainJson[f"{prefix}_called_imsi"])
        except:
            pass
        try:
            mainJson[f"{prefix}_called_number"] = chasher(mainJson[f"{prefix}_called_number"])
        except:
            pass
        try:
            mainJson[f"{prefix}_called_subs_last_ex_id"] = chasher(mainJson[f"{prefix}_called_subs_last_ex_id"])
        except:
            pass
        try:
            mainJson[f"{prefix}_calling_imsi"] = hasher(mainJson[f"{prefix}_calling_imsi"])
        except:
            pass
        try:
            mainJson[f"{prefix}_calling_number"] = chasher(mainJson[f"{prefix}_calling_number"])
        except:
            pass
        try:
            mainJson[f"{prefix}_calling_subs_last_ex_id"] = chasher(mainJson[f"{prefix}_calling_subs_last_ex_id"])
        except:
            pass
        try:
            mainJson[f"{prefix}_connected_to_number"] = chasher(mainJson[f"{prefix}_connected_to_number"])
        except:
            pass
        try:
            mainJson[f"{prefix}_destination_number"] = chasher(mainJson[f"{prefix}_destination_number"])
        except:
            pass
        try:
            mainJson[f"{prefix}_dialled_digits"] = chasher(mainJson[f"{prefix}_dialled_digits"])
        except:
            pass
        try:
            mainJson[f"{prefix}_forwarded_to_number"] = chasher(mainJson[f"{prefix}_forwarded_to_number"])
        except:
            pass
        try:
            mainJson[f"{prefix}_forwarding_imsi"] = hasher(mainJson[f"{prefix}_forwarding_imsi"])
        except:
            pass
        try:
            mainJson[f"{prefix}_orig_calling_number"] = chasher(mainJson[f"{prefix}_orig_calling_number"])
        except:
            pass
        try:
            mainJson[f"{prefix}_served_imsi"] = hasher(mainJson[f"{prefix}_served_imsi"])
        except:
            pass
        try:
            mainJson[f"{prefix}_served_number"] = chasher(mainJson[f"{prefix}_served_number"])
        except:
            pass
        try:
            mainJson[f"{prefix}_served_party_identity"] = chasher(mainJson[f"{prefix}_served_party_identity"])
        except:
            pass
        try:
            mainJson[f"{prefix}_called_imei"] = hasher(mainJson[f"{prefix}_called_imei"])
        except:
            pass
        try:
            mainJson[f"{prefix}_called_imeisv"] = hasher(mainJson[f"{prefix}_called_imeisv"])
        except:
            pass
        try:
            mainJson[f"{prefix}_calling_imei"] = hasher(mainJson[f"{prefix}_calling_imei"])
        except:
            pass
        try:
            mainJson[f"{prefix}_calling_imeisv"] = hasher(mainJson[f"{prefix}_calling_imeisv"])
        except:
            pass
        try:
            mainJson[f"{prefix}_forwarding_imei"] = hasher(mainJson[f"{prefix}_forwarding_imei"])
        except:
            pass
        try:
            mainJson[f"{prefix}_forwarding_imeisv"] = hasher(mainJson[f"{prefix}_forwarding_imeisv"])
        except:
            pass
        try:
            mainJson[f"{prefix}_outpulsed_number"] = chasher(mainJson[f"{prefix}_outpulsed_number"])
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
