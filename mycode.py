import sys
import operator
import re
import csv
import pprint
import numpy as np
import sqlparse as parser
from sqlparse.tokens import Keyword, DML,Wildcard,Whitespace
from sqlparse.sql import IdentifierList, Identifier,Where,Function

################### Global Variables#########################

tableDict={} # {table1:[columns],table2:[table2.b,table2.d]} from metadata file

################### Extracted from user's sql query ###################
tables=None  #[table1,table2]
whereClause=None # WHERE a>2
identifiers=None # [a,table2.b,c]
distinct=False   # True if distinct is present
aggregate=None   # MAX(a),MAX(table1.a) etc

# allowed aggregate functions
aggregateFunctions={"SUM":sum, "MAX":max,"MIN":min} #average is also allowed
operators={"<":operator.lt, "<=":operator.le, ">":operator.gt,">=":operator.ge,"=":operator.eq,"AND":operator.and_,"OR":operator.or_}

# data of tables present in database
# {table1:{table1.a:[values],
#		   table2.b:[values]
#		   },
#  table2: .....
# }
tableData={}

############# Prints Error #############
def printError(s):
	print(s)
	return False

############# Read Tables Metadata from metadata.txt #############
############# Prepare tableDict #############
def readMetaData():
	fp=open("metadata.txt", "r")
	line=fp.readline()
	table=False
	tableName=None
	while line:
		if "begin_table" in line:
			table=True
		elif "end_table" in line:
			table=False
		elif table:
			tableName=line.strip()#.upper()
			tableDict[tableName]=[]
			table=False
		else:
			col=tableName.upper()+"."+line.strip().upper()
			# col=line.strip().upper()
			tableDict[tableName].append(col)
		line = fp.readline()

############# Load table data in tableData #############
def loadTables():
	global tableDict
	global tableData
	# print("=============================")
	for table in tableDict:
		with open(table+".csv") as csvFile:
			# print(table)
			temp={}
			for col in tableDict[table]:
				temp[col]=[]
			csv_reader = csv.reader(csvFile, delimiter=',')
			for row in csv_reader:
				for index,val in enumerate(row):
					temp[tableDict[table][index]].append(int(val))
		# print("===========================")
		tableData[table.upper()]=temp
	tableDict={key.upper():tableDict[key] for key in tableDict.keys()}

############# Starting point of execution #############
def main():
	if len(sys.argv)>1:
		#read meta data file
		readMetaData()
		loadTables()
		#read command line
		sqlQueryStmt=parser.parse(sys.argv[1])[0]
		if validateQuery(sqlQueryStmt.tokens,tableDict):
			# print("Success")
			executeQuery()
		else:
			printError("")

	else:
		print("Provide sql query as argument")


############# Validates sql query and find out tables,attributes,whereClause etc #############
def validateQuery(tokens,tableDict):
	# pprint.pprint(tokens)
	# print(parser.sql.Comparison(tokens))
	global tables
	global whereClause
	global identifiers
	fromPresent,fromindex=extractFrom(tokens)
	if fromPresent==False:
		return printError(fromindex)
	slicedTokens=tokens[fromindex:]
	tablesPresent,tables,tablesindex=extractTable(slicedTokens)
	if tablesPresent==False:
		return printError("Invalid Query: No Table given ")
	identifierPresent,identifiers,identifierindex=extractTableIdentifiers(tokens[:fromindex])
	if identifierPresent==False and aggregate==None:
		return printError("Invalid Query: No columns provided ")


	if len(slicedTokens)-1==tablesindex:
		# print("A")
		return True
	elif len(slicedTokens)==tablesindex+2:
		# print("B")
		if slicedTokens[tablesindex+1].ttype is Whitespace:
			return True
		return printError("Invalid Query1")	
	elif len(slicedTokens)==tablesindex+3:
		# print("C")
		if slicedTokens[tablesindex+1].ttype is Whitespace:
			if isinstance(slicedTokens[tablesindex+2],Where):
				whereClause=slicedTokens[tablesindex+2].value.upper()
				return True
		return printError("Invalid Query: did you mean 'where' ")	
	else:	
		# print("D")
		return printError("Invalid Query")	


############# Execute query #############
############# Resolve Attributes #############
############# Join tables #############
def executeQuery():
	
	# print("tables",tables,"\nidentifiers= ",identifiers,"\nwhereClause= ",whereClause,"\ndistinct= ",distinct,"\naggregate= ",aggregate)
	# print(tableDict)
	for table in tables:
		if table not in tableDict.keys():
			return printError("Error: %s table doesn't exist "%(table))
	
	
	finalTable=None
	
	if len(tables)==1:
		#single table
		tableName=tables[0]
		finalTable=tableData[tableName]
	else:
		#multiple tables
		finalTable=crossProduct()


	################### handle where clause if exist ###################
	if whereClause:
		finalTable=handleWhere(finalTable)
	if finalTable==None:
		return False;

	###################################################################

	keys=finalTable.keys()  # table keys
	######################### resolve identifiers ie *, a, table2.a etc ##############
	resolvedIdentifiers=[]
	if "*" in identifiers:
		if len(identifiers)==1:
			resolvedIdentifiers=list(keys)
			# print(resolvedIdentifiers)
		else:
			return printError("Error:Invalid attributes")
	else:
		for attr in identifiers:
			attrSet=resolveIdentifier(attr)
			if len(attrSet)==0:
				#no valid attribute/ambiguous attr
				return False
			resolvedIdentifiers=resolvedIdentifiers+list(attrSet)
		# print(resolvedIdentifiers)

	######################### resolve identifiers ie *, a, table2.a etc ##############

	executeQueryOneTable(finalTable,resolvedIdentifiers)



############## Execute query finally on final joined/single table  #############
def executeQueryOneTable(finalTable,resolvedIdentifiers):
	# keys=finalTable.keys()
	dataDict={}
	if aggregate:
		#aggregate present
		aggFunt,aggAttr=getAggregateData(aggregate)
		aggAttrSet=resolveIdentifier(aggAttr)
		if len(aggAttrSet)==0:
			return False
		aggAttr=aggAttrSet.pop()
		if(aggFunt=="AVERAGE"):
			val=sum(finalTable[aggAttr])/len(finalTable[aggAttr])
			dataDict[aggregate]=[val]
			printOutput(dataDict)
		else:
			if aggFunt not in aggregateFunctions.keys():
				return printError("Error: not a valid aggregate function- "+aggFunt)
			funct=aggregateFunctions[aggFunt]
			val=funct(finalTable[aggAttr])
			dataDict[aggregate]=[val]
			printOutput(dataDict)
	else:
		if distinct:
			showDistinct(finalTable,resolvedIdentifiers)
		else:
			for attr in resolvedIdentifiers:
				dataDict[attr]=finalTable[attr]
			printOutput(dataDict)

############################### Helping functions to execute sql -starts ###############################

# get aggregate function and attribute
def  getAggregateData(aggregate):
	index=aggregate.find("(")
	return (aggregate[:index],aggregate[index+1:len(aggregate)-1])

# check of identifier in where is a number or a attribute
def checkNumberORAttr(first,second):
	isFirstAttr=True
	isSecondAttr=True
	try:
		first=int(first)
		isFirstAttr=False
	except:
		first=first
	try:
		second=int(second)
		isSecondAttr=False
	except:
		second=second
	# print(first,second,isFirstAttr,isSecondAttr)
	if isFirstAttr:
		first=resolveIdentifier(first)
		if len(first)==0:
			return (None,None,None,None)
		first=first.pop()
	if isSecondAttr:
		second=resolveIdentifier(second)
		if len(second)==0:
			return (None,None,None,None)
		second=second.pop()
	return (first,isFirstAttr,second,isSecondAttr)

# return filter data based on clauses, handles and or also
def filterDataOnClause(finalTable,clause1,clause2,logic):
	#check if first and second identifier is a number or attribute
	firstA,operatorA,secondA=clause1
	firstA,isFirstAAttr,secondA,isSecondAAttr=checkNumberORAttr(firstA,secondA)
	if firstA==None:
		return None
	#handle and or
	if logic!=None:
		firstB,operatorB,secondB=clause2
		firstB,isFirstBAttr,secondB,isSecondBAttr=checkNumberORAttr(firstB,secondB)
		if firstB==None:
			return None
	

	keys=list(finalTable.keys())
	resultantTable={ x:[] for x in keys}
	length=len(finalTable[keys[0]])

	# without and or
	if logic==None:

		if isFirstAAttr and isSecondAAttr:
			for i in range(0,length):
				if operators[operatorA](finalTable[firstA][i],finalTable[secondA][i]):
					for key in keys:
						resultantTable[key].append(finalTable[key][i])
		elif isFirstAAttr:
			for i in range(0,length):
				if operators[operatorA](finalTable[firstA][i],secondA):
					for key in keys:
						resultantTable[key].append(finalTable[key][i])
		elif isSecondAAttr:
			for i in range(0,length):
				if operators[operatorA](firstA,finalTable[secondA][i]):
					for key in keys:
						resultantTable[key].append(finalTable[key][i])
		else:
			for i in range(0,length):
				if operators[operatorA](firstA,secondA):
					for key in keys:
						resultantTable[key].append(finalTable[key][i])
		return resultantTable

	# and ,or, all combinations
	else:
		if isFirstAAttr and isSecondAAttr:
			if isFirstBAttr and isSecondBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](finalTable[firstA][i],finalTable[secondA][i]),operators[operatorB](finalTable[firstB][i],finalTable[secondB][i])):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			elif isFirstBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](finalTable[firstA][i],finalTable[secondA][i]),operators[operatorB](finalTable[firstB][i],secondB)):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			elif isSecondBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](finalTable[firstA][i],finalTable[secondA][i]),operators[operatorB](firstB,finalTable[secondB][i])):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			else:
				for i in range(0,length):
					if operators[logic](operators[operatorA](finalTable[firstA][i],finalTable[secondA][i]),operators[operatorB](firstB,secondB)):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			return resultantTable


		elif isFirstAAttr:
			if isFirstBAttr and isSecondBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](finalTable[firstA][i],secondA),operators[operatorB](finalTable[firstB][i],finalTable[secondB][i])):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			elif isFirstBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](finalTable[firstA][i],secondA),operators[operatorB](finalTable[firstB][i],secondB)):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			elif isSecondBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](finalTable[firstA][i],secondA),operators[operatorB](firstB,finalTable[secondB][i])):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			else:
				for i in range(0,length):
					if operators[logic](operators[operatorA](finalTable[firstA][i],secondA),operators[operatorB](firstB,secondB)):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			return resultantTable


		elif isSecondAAttr:
			if isFirstBAttr and isSecondBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](firstA,finalTable[secondA][i]),operators[operatorB](finalTable[firstB][i],finalTable[secondB][i])):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			elif isFirstBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](firstA,finalTable[secondA][i]),operators[operatorB](finalTable[firstB][i],secondB)):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			elif isSecondBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](firstA,finalTable[secondA][i]),operators[operatorB](firstB,finalTable[secondB][i])):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			else:
				for i in range(0,length):
					if operators[logic](operators[operatorA](firstA,finalTable[secondA][i]),operators[operatorB](firstB,secondB)):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			return resultantTable


		else:
			if isFirstBAttr and isSecondBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](firstA,secondA),operators[operatorB](finalTable[firstB][i],finalTable[secondB][i])):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			elif isFirstBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](firstA,secondA),operators[operatorB](finalTable[firstB][i],secondB)):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			elif isSecondBAttr:
				for i in range(0,length):
					if operators[logic](operators[operatorA](firstA,secondA),operators[operatorB](firstB,finalTable[secondB][i])):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			else:
				for i in range(0,length):
					if operators[logic](operators[operatorA](firstA,secondA),operators[operatorB](firstB,secondB)):
						for key in keys:
							resultantTable[key].append(finalTable[key][i])
			return resultantTable


# handle where clause (parsing)
def handleWhere(finalTable):
	# clause :=  clause and clause
	# clause :=  clause or clause
	# clause := name operator value
	# operator := = | <> | < | <= | > | >=
	# print(whereClause)
	a=re.search("WHERE (.*) (AND|OR) (.*)",whereClause)
	clause1=None
	clause2=None
	logic=None
	if a:
		clause1=a.group(1).strip()
		logic=a.group(2).strip()
		clause2=a.group(3).strip()
	else:
		a=re.search("WHERE (.*)",whereClause)
		if a:
			clause1=a.group(1).strip()
		else:
			print("Error in where clause")
			return None

	# print(clause1,logic,clause2)
	if (clause1==None or len(clause1)==0) and (clause2==None or len(clause2)==0):
		print("Error in where clause")
		return None
	if logic==None:
		#only one clause
		# a=re.search("(.*)(<=|>=|>|<|=)(.*)",clause1)
		a=re.search("([^=><]*)(=|>=?|<=?)(.*)",clause1)
		if a:
			clause1=(a.group(1).strip(),a.group(2).strip(),a.group(3).strip())
		else:
			print("Error in where clause "+ clause1)
			return None
	else:
		#and or present
		a=re.search("([^=><]*)(=|>=?|<=?)(.*)",clause1)
		b=re.search("([^=><]*)(=|>=?|<=?)(.*)",clause2)
		if a:
			clause1=(a.group(1).strip(),a.group(2).strip(),a.group(3).strip())
		else:
			print("Error in where clause "+clause1)
			return None
		if b:
			clause2=(b.group(1).strip(),b.group(2).strip(),b.group(3).strip())
		else:
			printError("Error in where clause "+clause2)
			return None

	finalTable=filterDataOnClause(finalTable,clause1,clause2,logic)
	# return table or None
	return finalTable
				

# return cross product of 2 tables
def product(table1,table2):

	table1keys=list(table1.keys())
	table2keys=list(table2.keys())
	table1Size=len(table1[table1keys[0]])
	table2Size=len(table2[table2keys[0]])
	if table1Size==0 or table2Size==0:
		return printError("Cannot join tables: One of the table is empty")
	for key in table1keys:
		table1[key]=table1[key]*table2Size
	for key in table2keys:
		table1[key]=[item for item in table2[key] for i in range(table1Size)]
	return table1

# return cross product of n tables
def crossProduct():
	for i,tableName in enumerate(tables):
		if i==0:
			table1=tableData[tableName]
			continue
		table2=tableData[tableName]
		table1=product(table1,table2)
	return table1
	

# execute sql with "distinct" keyword
def showDistinct(finalTable,resolvedIdentifiers):
	length=len(finalTable[resolvedIdentifiers[0]])
	mytable={key:[] for key in resolvedIdentifiers}
	mylist=[]
	for i in range(0,length):
		temp=[]
		for key in resolvedIdentifiers:
			temp.append(finalTable[key][i])
		if temp not in mylist:
			mylist.append(temp)
	printRows(resolvedIdentifiers,mylist)


# associate an attribute given by user to table i.e. table1's col => table1.col
def getAttr(tableName,attr):
	if "." in attr:
		return attr
	else:
		return tableName+"."+attr

# validates attributes : unknown/ambiguous and associate them with corresponding tables
def resolveIdentifier(attr):

	temp=set()
	isExist=False
	for tableName in tables:
		attrtemp=getAttr(tableName,attr)
		if attrtemp in tableDict[tableName]:
			isExist=True
			temp.add(attrtemp)
			if len(temp)>1:
				attrtemp=temp.pop()
				print("Error: ambiguous attribute "+attr)
				return set()
	if not(isExist):
		print("Error: Invalid attribute "+attr)
		return set()
	return temp

############################### Helping functions to execute sql -ends ###############################

####################################### Printing Methods starts #######################################

############# Prints collection of lists parallely #############
def printRows(attrs,listRows):
	print("=================================================================")
	for a in attrs:
		print("%15s  |"%(a),end=" ")
	print()
	print("=================================================================")
	for row in listRows:
		for val in row:
			print("%15s  |"%(val),end=" ")
		print()
	print("=================================================================")


############# Prints collection of lists as seperate columns #############
def printOutput(dataDict):
	print("=================================================================")
	# pprint.pprint(dataDict)
	keys=list(dataDict.keys())

	for key in keys:
		print("%15s  |"%(key),end=" ")
	print()
	print("=================================================================")
	length=len(dataDict[keys[0]])
	for i in range(0,length):
		for key in keys:
			print("%15s  |"%(dataDict[key][i]),end=" ")
		print()
	print("=================================================================")

####################################### Printing Methods ends #######################################

####################################### SqlParsing Methods starts #######################################

# check if from,aggregate,distinct is present or not
def extractFrom(tokens):
	frm=False
	ind=None
	isDMLAbsent=True

	global distinct
	global aggregate
	for i,item in enumerate(tokens):
		if item.ttype is Keyword :
			if item.value.upper()=="FROM":
				frm=True
				ind=i
			elif item.value.upper()=="DISTINCT":
				distinct=True
		elif item.ttype is DML:
			isDMLAbsent=False
		elif isinstance(item,Function):
			aggregate=item.value.upper()
	if isDMLAbsent:
		return (False,"Invalid Query: Error: select keyword")
	if frm:
		return (True,ind)
	return (False,"Invalid Query: No from clause")

# return list of extracted tables
def extractTable(tokens):
	return extractTableIdentifiers(tokens)

# return list of extracted identifiers
def extractTableIdentifiers(tokens):
	tables=[]
	for i,tokenitem in enumerate(tokens):
		if isinstance(tokenitem,IdentifierList):
			for item in tokenitem.get_identifiers():
				tables.append(item.value.upper())
			return (True,tables,i)
		elif isinstance(tokenitem,Identifier):
			tables.append(tokenitem.value.upper())
			return (True,tables,i)
		elif tokenitem.ttype is Wildcard:
			tables.append(tokenitem.value.upper())
			return (True,tables,i)
	return (False,[],i)

####################################### SqlParsing Methods ends #######################################


############# Main #############
if __name__ == "__main__":
	main()