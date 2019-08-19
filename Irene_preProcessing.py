import datetime
from datetime import datetime
import pyodbc
import getpass
import nltk
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import string
import numpy
from textblob.classifiers import NaiveBayesClassifier
from textblob import TextBlob


print("Hi %s.. I'm IRENE! \n"%(getpass.getuser()))

print("CRM Notes Mining begins \n-----------------------")

InpTable="dbo.t_PyInput"        #Notes Input Table
PrcTable="dbo.t_PyProcessing"   #Processing Table
TrainTable="dbo.t_PyTrain"      #Training data set 

print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Connecting to SQL Server")

con=pyodbc.connect('DRIVER={SQL Server};Server=V-KACH;Database=CRMNotes;Trusted_Connection=yes;')
cur=con.cursor()

qry="IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s; "%(PrcTable,PrcTable)
cur.execute(qry)

qry="CREATE TABLE %s (ID int, Description nvarchar(MAX), DescriptionStopWords nvarchar(MAX), IsLeftCompany nvarchar(10), DescriptionPOS nvarchar(MAX))"%(PrcTable)
cur.execute(qry)

print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Processing Table Created")

#print(qry)


#Get recent Notes (Delta logic from last execution)

#===============================================================================
# cur.execute("select count(*) cnt from %s(nolock)"%(InpTable))
# notesCount=cur.fetchone()
# print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Notes found in Input table :",(notesCount.cnt))
#===============================================================================


#Pre-processing Input notes
#@SQL : Stripe all newline char
qry="""update %s set [Description]=replace(replace([Description], char(13),'. '),char(10),'. ')"""%(InpTable)
cur.execute(qry)

#Custom stowords list
#stopset = set(stopwords.words('english'))
stopwords_custom={'the', 'these', 'ourselves', 'hers', 'are', 'most', 'by', 'other', 'i', 'so', 'having', 'our', 'under', 'did', 'whom', 'will', 'such', 'don', 'do', 'can', 't', 'where', 'once', 'during', 'its', 'to', 'how', 'this', 'they', 'against', 'himself', 'for', 'all', 'yourselves', 'while', 'but', 'that', 'any', 'had', 'again', 'more', 'very', 'am', 'it', 'before', 'my', 'we', 'herself', 'yours', 'as','there','here', 'only', 'because', 'into', 'than', 'have', 'from', 'of', 'an', 'above', 'has', 'your', 'now', 'which', 'theirs', 'you', 'and', 'few', 'ours', 'with', 's', 'up', 'off', 'yourself', 'on', 'their', 'at', 'after', 'out', 'themselves', 'if', 'why', 'was', 'not', 'through', 'then', 'in', 'were', 'itself', 'what', 'does', 'a', 'between', 'when', 'same', 'or', 'over', 'been', 'who', 'just', 'down', 'each', 'too', 'own', 'is', 'be', 'doing', 'below', 'both', 'myself', 'further', 'those', 'should', 'about', 'until', 'nor', 'some', 'being'}

print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Text Mining begins, Sit back and relax!")

#Get train Set
cur.execute("""SELECT top 50 DescriptionStopwords,IsLeftCompany FROM dbo.t_PyTrain where isleftcompany='y' union all select top 100 DescriptionStopwords,IsLeftCompany FROM dbo.t_PyTrain where isleftcompany='n'""")
TrainSet=cur.fetchall()

#Train NaiveBayes 
cl = NaiveBayesClassifier(TrainSet)
print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Irene's Training knowledgebase refreshed")


cur.execute('select top 3 [ID],[Description] from [dbo].[t_PyInput]')
notes = cur.fetchall()
for note in notes:
    #notetxt=(note.Description)
    sentence_list=sent_tokenize(note.Description.strip())
    for sentnce in sentence_list:
        S2=''.join(map(str, sentnce))
        #text=S2.lower() #to lower case
        tokens1=word_tokenize(str(S2.lower()))
        tokens2 = [w for w in tokens1 if not w in stopwords_custom] # remove stop words
        T2=" ".join(tokens2)
        if len(T2)>7:
            PredictIsLeftCompany=cl.classify(T2)
            if PredictIsLeftCompany == "N":
                cur.execute("""INSERT INTO t_PyProcessing (ID,Description,DescriptionStopWords,IsLeftCompany) VALUES(?,?,?,?)""",(note.ID,S2,T2,PredictIsLeftCompany))
            else:
                #tagged=nltk.sent_tokenize(sentnce.strip())
                #tagged=[nltk.word_tokenize(sent) for sent in tagged]
                #tagged=[nltk.pos_tag(sent) for sent in tagged]
                tagged=[nltk.pos_tag(tokens1)]
                tagged=''.join(map(str,tagged))
                cur.execute("""INSERT INTO t_PyProcessing (ID,Description,DescriptionStopWords,IsLeftCompany,DescriptionPOS) VALUES(?,?,?,?,?)""",(note.ID,S2,T2,PredictIsLeftCompany,tagged))
                


#===============================================================================
# print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Pre-processing initiated")
# qry="Delete FROM %s where len(description)<=5"%(PrcTable)
# cur.execute(qry)
#===============================================================================


#===============================================================================
# qry="""SELECT top 100 DescriptionStopwords,IsLeftCompany FROM dbo.t_PyTrain where isleftcompany='y' union all select top 100 DescriptionStopwords,IsLeftCompany FROM dbo.t_PyTrain where isleftcompany='n'"""
# cur.execute(qry)
# TrainSet=cur.fetchall()
# cl = NaiveBayesClassifier(TrainSet)
# PredictIsLeftCompany=cl.classify("kevin chan is no longer interested in teh opportunity")  
#===============================================================================

print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Classification Completed")


#===============================================================================
# qry="""alter table [dbo].[t_PyProcessing] add DescriptionPOS nvarchar(MAX)"""
#===============================================================================



del cur
con.commit()
con.close()


