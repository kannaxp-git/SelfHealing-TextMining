#importing necessary packages
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
import string
import re
import pprint
import ast
from nltk.tree import Tree
from nltk.text import Text  
import itertools
import nltk.tag

#grammar rules for identifying noun phrase
patterns = """CHUNK:{<JJ>+<NNP?>+}
{<NNP?>+<CC><NNP?>+}
{<NNP?>+}


"""
#parsing grammar
NPChunker = nltk.RegexpParser(patterns)    

   
def traverse(t):
    #print("in traverse")
    for n in t:
        if isinstance(n,nltk.tree.Tree):
            if n.label()=='CHUNK':
                #print('yes it is a CHUNK :',n.leaves(),end='\n')
                np.append(n.leaves())
            #print(n.label(),"----------------is subtree------------------------",end="\n")
            #print("going into subtree",end='\n')
            traverse(n)



def flatten(data):
    #print("in flatten")
    final=[]
    for lists in data:
        lg=len(lists)
        temp=''
        for i in range(0,lg):
            temp=temp+lists[i][0]+' '
        final.append(temp)

    return final
        
   
     



print("Hi %s.. I'm IRENE! \n"%(getpass.getuser()))

print("CRM Notes Mining begins \n-----------------------")

#Notes Input Table

InpTable="[dbo].[t_PyInput]"       #input table

PrcTable="[dbo].[t_PyProcessing]"   #Processing Table




print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Connecting to SQL Server for POS tagging")

con=pyodbc.connect('DRIVER={SQL Server};Server=DUBCPDMSQL08.EUROPE.CORP.MICROSOFT.COM;Database=KBASE;Trusted_Connection=yes;')
cur=con.cursor()

qry="IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s; "%(PrcTable,PrcTable)
cur.execute(qry)


qry="CREATE TABLE %s (noteID nvarchar(100), Description nvarchar(MAX), DescriptionStopWords nvarchar(MAX), IsLeftCompany nvarchar(10), DescriptionPOS nvarchar(MAX))"%(PrcTable)
cur.execute(qry)


print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Processing Table Created")

qry="""update %s set [Description]=replace(replace([Description], char(13),'. '),char(10),'. ')"""%(InpTable)
cur.execute(qry)

#Custom stowords list
#stopset = set(stopwords.words('english'))
stopwords_custom={'the', 'these', 'ourselves', 'hers', 'are', 'most', 'by', 'other', 'i', 'so', 'having', 'our', 'under', 'did', 'whom', 'will',
                  'such', 'don', 'do', 'can', 't', 'where', 'once', 'during', 'its', 'to', 'how', 'this', 'they', 'against', 'himself', 'for', 'all', 'yourselves',
                  'while', 'but', 'that', 'any', 'had', 'again', 'more', 'very', 'am', 'it', 'before', 'my', 'we', 'herself', 'yours', 'as','there','here', 'only',
                  'because', 'into', 'than', 'have', 'from', 'of', 'an', 'above', 'has', 'your', 'now', 'which', 'theirs', 'you', 'and', 'few', 'ours', 'with', 's',
                  'up', 'off', 'yourself', 'on', 'their', 'at', 'after', 'out', 'themselves', 'if', 'why', 'was', 'not', 'through', 'then', 'in', 'were', 'itself',
                  'what', 'does', 'a', 'between', 'when', 'same', 'over', 'been', 'who', 'just', 'down', 'each', 'too', 'own', 'is', 'be', 'doing', 'below',
                  'both', 'myself', 'further', 'those', 'should', 'about', 'until', 'nor', 'some', 'being'}

print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Text Mining begins, Sit back and relax!")

#Get train Set

cur.execute("""select top 800 [stop],[left]  from [dbo].[t_PyYesTrain] where len([stop])>8  union all select  top 800 [stop],[left]  from [dbo].[t_PyNoTrain] where len([stop])>8""")

TrainSet=cur.fetchall()


print("training the classifier.....")
cl = NaiveBayesClassifier(TrainSet)
print("training completed")

print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Irene's Training knowledgebase refreshed")

print(datetime.strftime(datetime.now(), '%H:%M:%S')+"Classification Begins")

cur.execute(' SELECT   [noteID] ,[Description] FROm [dbo].[t_PyInput] where [description] is not null ')
notes = cur.fetchall()
for note in notes:
    #sentence tokenization
    sentence_list=sent_tokenize(note.Description.strip())
    for sentnce in sentence_list:
        S2=''.join(map(str, sentnce))
        #word tokenization
        tokens1=word_tokenize(str(S2))
        tokens2 = [w for w in tokens1 if not w in stopwords_custom] # remove stop words
        T2=" ".join(tokens2)
        if len(T2)>7:
            PredictIsLeftCompany=cl.classify(T2)
            if PredictIsLeftCompany == "Y":
                 
              	 #pos tagging of yes labelled sentences	
                 tagged=[nltk.pos_tag(tokens1)]
                 tagged=''.join(map(str,tagged))
                 cur.execute("""INSERT INTO [dbo].[t_PyProcessing] (noteid,Description,DescriptionStopWords,IsLeftCompany,DescriptionPOS) VALUES(?,?,?,?,?)""",	(note.noteID,S2,T2,PredictIsLeftCompany,tagged))
                
            else:
                #No labelled sentences
                cur.execute("""INSERT INTO [dbo].[t_PyProcessing] (noteid,Description,DescriptionStopWords,IsLeftCompany) VALUES(?,?,?,?)""",(note.noteID,S2,T2,PredictIsLeftCompany))


print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Classification Completed")

del cur
con.commit()
con.close()


##################


print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Connecting to SQL Server for entity resolution")

con=pyodbc.connect('DRIVER={SQL Server};Server=DUBCPDMSQL08.EUROPE.CORP.MICROSOFT.COM;Database=KBASE;Trusted_Connection=yes;')
cur=con.cursor()

PrcTable2="[dbo].[t_PyMSX_SMB_ExactMatch]"

qry="IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s; "%(PrcTable2,PrcTable2)
cur.execute(qry)

#creating table for storing exact matching  contacts
qry="CREATE TABLE %s (noteID nvarchar(100), Description nvarchar(MAX), DescriptionStopWords nvarchar(MAX), IsLeftCompany nvarchar(10),DescriptionPOS nvarchar(MAX),Entity nvarchar(100),CrmCreatedDate datetime,RegardingEntityName nvarchar(255),RegardingObjectId uniqueidentifier,ContactId uniqueidentifier,Status nvarchar(50),MatchPercentage int)"%(PrcTable2)
cur.execute(qry)

#picking yes labelled rows from intermediate processing table
cur.execute(""" SELECT  [noteID]
      ,[Description]
      ,[DescriptionStopWords]
      ,[IsLeftCompany]
      ,[DescriptionPOS] FROm [dbo].[t_PyProcessing] where [IsLeftCompany]='y' """)
notes = cur.fetchall()



print(datetime.strftime(datetime.now(), '%H:%M:%S')+" starting with tokeninzing part")


for note in notes:
    sample=note.Description
    tagged=note.DescriptionPOS
##    
##detecting whether sentence contains our target phrases such as 'left'  
    inx=-1
    if (("no" and  "longer") in sample):
        inx=sample.find("no")
        #print("index of 'no longer' is",inx)         
    elif(("no" and "long") in sample):
        inx=sample.find("no")
        #print("index of 'no long' is",inx)   
    elif ("left" in sample):
        inx=sample.find("left")
        #print("index of 'left' is" ,inx) 
    

    if(inx==-1):
                 #print("not found")
                 cur.execute("""INSERT INTO dbo.[t_PyMSX_SMB_ExactMatch] (noteID,Description,DescriptionStopWords,IsLeftCompany,DescriptionPOS) VALUES(?,?,?,?,?)""",(note.noteID,note.Description,note.DescriptionStopWords,note.IsLeftCompany,note.DescriptionPOS))
   
   
    else:
       
	#target phrase found,hence chunking the sentence as per grammer rules
        np=[] 

        npose= ast.literal_eval(tagged)
        #print(npose,"\n")

        #print("\n chunking")
        tree = [NPChunker.parse(npose)]
        
        #print(tree)

    

        
        #print("calling traverse fn.")
        traverse(tree)
        #print("FINAL NP LIST IS",np)

        #print("flattening np")
        final=flatten(np)
        #print("flattened np is",final)
	#finding the noun phrase that has min distance from target phrase
        mindiff=200
        select=''
        for i in final:
            j=''.join(i)
            index=sample.find(j)
           
        
            diff=inx-index
            if diff>=0:
                if diff<mindiff:
                    mindiff=diff
                    select=j
        #if the noun phrase has 'and' 'or' then inserting 2 rows in table for 2 entities extracted          
        if ((' and ') in select):
            
            ind=select.find(" and ")
            
            length=len(select)
            s1=select[0:ind]
            
            s2=select[ind+5:length]
            
            
            cur.execute("""INSERT INTO [dbo].[t_PyMSX_SMB_ExactMatch] (noteID,Description,DescriptionStopWords,IsLeftCompany,DescriptionPOS,Entity) VALUES(?,?,?,?,?,?)""", (note.noteID,note.Description,note.DescriptionStopWords,note.IsLeftCompany,note.DescriptionPOS,s1))
            
            cur.execute("""INSERT INTO [dbo].[t_PyMSX_SMB_ExactMatch] (noteID,Description,DescriptionStopWords,IsLeftCompany,DescriptionPOS,Entity) VALUES(?,?,?,?,?,?)""", (note.noteID,note.Description,note.DescriptionStopWords,note.IsLeftCompany,note.DescriptionPOS,s2))

        elif ((' or ') in select):
            ind=select.find(" or ")
            
            length=len(select)
            s1=select[0:ind]
            
            s2=select[ind+5:length]
            
            
            cur.execute("""INSERT INTO [dbo].[t_PyMSX_SMB_ExactMatch] (noteID,Description,DescriptionStopWords,IsLeftCompany,DescriptionPOS,Entity) VALUES(?,?,?,?,?,?)""", (note.noteID,note.Description,note.DescriptionStopWords,note.IsLeftCompany,note.DescriptionPOS,s1))
            
            cur.execute("""INSERT INTO [dbo].[t_PyMSX_SMB_ExactMatch] (noteID,Description,DescriptionStopWords,IsLeftCompany,DescriptionPOS,Entity) VALUES(?,?,?,?,?,?)""", (note.noteID,note.Description,note.DescriptionStopWords,note.IsLeftCompany,note.DescriptionPOS,s2))
       
            

        else:
            cur.execute("""INSERT INTO [dbo].[t_PyMSX_SMB_ExactMatch] (noteID,Description,DescriptionStopWords,IsLeftCompany,DescriptionPOS,Entity) VALUES(?,?,?,?,?,?)""", (note.noteID,note.Description,note.DescriptionStopWords,note.IsLeftCompany,note.DescriptionPOS,select))
          
                
            

         
            
                
                
#exact match table ready
print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Entity Recognition Completed")

print(datetime.strftime(datetime.now(), '%H:%M:%S')+"preparing for approx and exact match")

#calling the stored procedure for carrying out the sql queries for approximate match
cur.execute(""" exec [kbase].[dbo].[sp_ent]  """) 

print(datetime.strftime(datetime.now(), '%H:%M:%S')+" Approximate and exact match tables are ready ")

del cur
con.commit()
con.close()



#############################




    
