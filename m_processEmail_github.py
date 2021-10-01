#m_processEmail

import requests #for extracting https data
import json 
import datetime

class EmailFormatter:
    """ collects the https response from the provided address and operates on the data """
    def __init__(self,
                 str_address,
                 storeLocal = True,
                 storageLocation = "C:\\Users\\Postulio\\Documents\\ordoroStuff\\",
                 tmpFileName = "tmp.txt",
                 refreshRawData = True):
        """ initialization collects the https response and stores it """
        self.str_address = str_address
        self.storeLocal = storeLocal
        self.storageLocation = storageLocation
        self.tmpFileName = tmpFileName
        self.refreshRawData = refreshRawData
        if self.refreshRawData == True:
            self.rawData = requests.get(self.str_address)
            self.tmpFile = open(self.tmpFileName, "w")
            self.tmpFile.write(self.rawData.text)
            self.tmpFile.close()
        else:
            print("keeping stale data")
   
    def getDistinctEmails(self, tmpFilePath, outFilePath):
        """
        returns distinct email addresses sorted alphabetically in the json data stored at tmpFilePath
        also saves the distinct email list to outFilePath
        blank/NoneType entries are ignored
        """
        self.tmpFilePath = tmpFilePath
        self.outFilePath = outFilePath
        tmpData = open(self.tmpFilePath, "r")
        str_tmpData = tmpData.read()
        tmpData.close()
        jsonData = json.loads(str_tmpData)
        distinctEmails = set() #use a set so that duplicates cannot be entered
        for d_data in jsonData['data']: # only one dictionary at the top level of the json structure.
            for entry in d_data:
                #print("entry, d_data[entry] is: {}, {}".format(entry, d_data[entry]))
                if entry == 'email':
                    email = d_data[entry]
                    if email is None: #skip email entries that are NoneType
                        continue
                    email = email.strip() #remove leading/trailing spaces
                    distinctEmails.add(email)
        #turn the set into a list so it can be sorted
        l_distinctEmails = list(distinctEmails)
        l_distinctEmails.sort()
        outFile = open(outFilePath, "w")
        for item in l_distinctEmails:
            outFile.write(item + '\n')
        outFile.close()
        return l_distinctEmails
        
    def getDomainPareto(self, distinctEmailsFilePath, outFilePath):
        """
        provide a path to a distinct emails list to return a descending list of domains with more than 1 user
        results are also stored to the outFilePath
        """
        self.distinctEmailsFilePath = distinctEmailsFilePath
        self.outFilePath = outFilePath
        emailFile = open(self.distinctEmailsFilePath, "r")
        fileContents = emailFile.readlines()
        d_pareto = {}
        for email in fileContents:
            atPos = email.find('@')
            domain = email[atPos+1:].replace("\n", "") #read all characters after the @ symbol
            if domain in d_pareto:
                d_pareto[domain] = d_pareto[domain] + 1
            else:
                d_pareto[domain] = 1
        d_pareto2 = {} #dict for storing domains with 2 or more uses
        for domain in d_pareto:
            #copy values to new dictionary if value is > 1
            if d_pareto[domain] > 1:
                d_pareto2[domain] = d_pareto[domain]
        #sort the dictionary
        d_pareto2 = dict(sorted(d_pareto2.items(), key=lambda item: item[1]))
        #reverse the dictionary
        d_pareto2 = dict(reversed(list(d_pareto2.items())))
        outFile = open(self.outFilePath, "w")
        for key in d_pareto2:
            csvStr = key + ", " + str(d_pareto2[key])
            print(csvStr)
            outFile.write(csvStr + '\n')
        outFile.close()
        return d_pareto2
    
    def getUsersInMonth(self, month, tmpFilePath, outFilePath):
        """
        provide a path to the raw json data of users and login times
        finds users that logged in during a specific month in UTC timezone
        returns a list and results are also stored at outFilePath
        """
        self.tmpFilePath = tmpFilePath
        self.outFilePath = outFilePath
        tmpData = open(self.tmpFilePath, "r")
        str_tmpData = tmpData.read()
        tmpData.close()
        jsonData = json.loads(str_tmpData)
        distinctEmails = set() #use a set so that duplicates cannot be entered
        s_usersInMonth = set() #create a set for storing distinct users in month
        for d_data in jsonData['data']: # only one dictionary at the top level of the json structure.
            for entry in d_data:
                #print("entry, d_data[entry] is: {}, {}".format(entry, d_data[entry]))
                email = ''
                if entry == 'email':
                    email = d_data[entry]
                elif entry == 'login_date':
                    loginDate = d_data[entry]
                if email is None or loginDate is None: #ignore entries with a missing email or missing date
                    continue
                email = email.strip()
                loginDate = loginDate.strip()
                if len(loginDate) > 0 and len(email) > 0:
                    #normalize all datetimes to UTC first
                    dt_loginDate = datetime.datetime.fromisoformat(loginDate) #convert string to datetime object
                    int_tzValue = int(loginDate[20:22])
                    if loginDate.find('-',19) > 0: 
                        dt_utcLoginDate = dt_loginDate + datetime.timedelta(hours=int_tzValue)
                    elif loginDate.find('+',19) > 0:
                        dt_utcLoginDate = dt_loginDate - datetime.timedelta(hours=int_tzValue)    
                    dt_utcLoginDate = dt_utcLoginDate.replace(tzinfo=datetime.timezone.utc)
                    #check which users logged in during a specific month
                    userMonth = dt_utcLoginDate.month
                    if userMonth == month: 
                        s_usersInMonth.add(email)
        l_usersInMonth = list(s_usersInMonth)
        l_usersInMonth.sort()
        #save the user in month list to txt file 
        outFile = open(self.outFilePath, "w")
        for item in l_usersInMonth:
            outFile.write(item + '\n')
        outFile.close()
        return l_usersInMonth

    def composeJson(self, emailAddress, uniqueEmailsFile, paretoFile, aprilEmailsFile):
        """ combines the text files into a single string for submittal as json """
        self.emailAddress = emailAddress
        self.uniqueEmailsFile = uniqueEmailsFile
        self.paretoFile = paretoFile
        self.aprilEmailsFile = aprilEmailsFile        
        #open unique emails data file
        uniqueEmailsData = open(self.uniqueEmailsFile, "r")
        l_uniqueEmailsData = uniqueEmailsData.readlines()
        uniqueEmailsData.close()
        #open domain counts file
        domainCountsData = open(self.paretoFile, "r")
        l_domainCountsData = domainCountsData.readlines()
        domainCountsData.close()
        #open april emails file
        aprilEmailsData = open(self.aprilEmailsFile, "r")
        l_aprilEmailsData = aprilEmailsData.readlines()
        aprilEmailsData.close()
        #add unique emails data to json
        lineCount = len(l_uniqueEmailsData)

        j = "{"
        j = j + "'your_email_address': '" + self.emailAddress + "', "
        j = j + "'unique emails': ["
        currentLine = 0
        for line in l_uniqueEmailsData:
            currentLine = currentLine + 1
            if currentLine < lineCount:
                j = j + "'" + line.replace("\n", "") + "',"
            else:
                j = j + "'" + line.replace("\n", "")
        j = j + "],"
        j = j + "'user_domain_counts': {"
        
        #add domain count info to json
        lineCount = len(l_domainCountsData)
        currentLine = 0
        for line in l_domainCountsData:
            currentLine = currentLine + 1
            lineSplit = line.split(',')
            domain = lineSplit[0]
            domainCount = lineSplit[1]
            if currentLine < lineCount:
                j = j + "'" + domain + "': " + "'" + domainCount.strip() + "',"
            else:
                j = j + "'" + domain + "': " + "'" + domainCount.strip() + "'"
        j = j + "},"
        #add april emails to json
        j = j + "'april_emails:' ["
        lineCount = len(l_aprilEmailsData)
        currentLine = 0
        for line in l_aprilEmailsData:
            currentLine = currentLine + 1
            if currentLine < lineCount:
                j = j + "'" + line.strip() + "',"
            else:
                j = j + "'" + line.strip() + "'"
        j = j + "]"
        j = j + "}"
        return j
    
    def submitJson(self, url, str_json):
        """ submits a string of json data to the provided url """
        self.url = url
        self.str_json = str_json
        submitResponse = requests.post(self.url, json = self.str_json )
        #submitResponse = requests.post(self.url, data = self.str_json )
        return submitResponse
        
#**************************************************************
            
#sample class usage

apiAddress = "https://us-central1-marcy-playground.cloudfunctions.net/ordoroCodingTest"
tmpEmailsPath = r"C:\Users\Postulio\Documents\ordoroStuff\tmp.txt"
distinctEmailsPath = r"C:\Users\Postulio\Documents\ordoroStuff\distinctEmails.txt"
emailParetoPath = r"C:\Users\Postulio\Documents\ordoroStuff\pareto.txt"
usersInMonthPath = r"C:\Users\Postulio\Documents\ordoroStuff\usersInApril.txt"

#get the list of distinct emails and the distinct email count
email = EmailFormatter(apiAddress, refreshRawData = False)
l_distinctEmails = email.getDistinctEmails(tmpEmailsPath, distinctEmailsPath)
distinctEmailCount = len(l_distinctEmails)
print("distinctEmailCount is: {}".format(distinctEmailCount))

#get a pareto of domains used more than once
d_domainPareto = email.getDomainPareto(distinctEmailsPath, emailParetoPath)
print("d_domainPareto is: {}".format(d_domainPareto))

#get users in month of april
email.getUsersInMonth(4, tmpEmailsPath, usersInMonthPath)

#compose the json response
emailAddress = 'someEmail@gmail.com'
jsonResult = email.composeJson(emailAddress, distinctEmailsPath, emailParetoPath, usersInMonthPath)
print("jsonResult is: ")
print(jsonResult)
response = email.submitJson(apiAddress, jsonResult)
print("response is: ")
print(response)






    

    
            
        
            
        
        
        
        
