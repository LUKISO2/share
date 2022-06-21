import sys
import datetime

def updateTime():
    try:
        arg = sys.argv
        date = int(arg[1])
        strDate = str(date)
        if len(strDate) != 14:
            print("Invalid date format")
            return
        toAdd = int(arg[2])
        print(datetime.datetime(int(strDate[0:4]),int(strDate[4:6]),int(strDate[6:8]),int(strDate[8:10]),int(strDate[10:12]),int(strDate[12:14])) + datetime.timedelta(seconds=toAdd))
    except:
        print("Error: Invalid date format")

if __name__ == "__main__":
    updateTime()
