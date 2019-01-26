import sys
import re
import csv
import pprint
import numpy as np
import sqlparse as parser
from sqlparse.tokens import Keyword, DML,Wildcard,Whitespace
from sqlparse.sql import IdentifierList, Identifier,Where,Function
tableDict={}
tables=None
whereClause=None
identifiers=None
distinct=False
aggregate=None
aggregateFunctions={"SUM":sum, "AVERAGE":"anuj", "MAX":max,"MIN":min}
tableData={}

def printError(s):
	print(s)
	return False

def main():
	if len(sys.argv)>1:
		#read meta data file
		readMetaData()
		loadTables()
		# pprint.pprint(tableData)
		#read command line
		# print(tableDict)
		sqlQueryStmt=parser.parse(sys.argv[1])[0]
		if validateQuery(sqlQueryStmt.tokens,tableDict):
			print("Success")
			executeQuery()
		else:
			printError("")

	else:
		print("Provide sql query as argument")

def  getAggregateData(aggregate):
	index=aggregate.find("(")
	return (aggregate[:index],aggregate[index+1:len(aggregate)-1])

def executeQuery():
	print("tables",tables,"\nidentifiers= ",identifiers,"\nwhereClause= ",whereClause,"\ndistinct= ",distinct,"\naggregate= ",aggregate)
	print(tableDict)
	for table in tables:
		if table not in tableDict.keys():
			return printError("Error: %s table doesn't exist "%(table))
	dataDict={}
	################################### single table######################
	if len(tables)==1:

		tableName=tables[0]
		keys=tableData[tableName].keys()
		if len(identifiers)==1 and identifiers[0]=="*":
			printOutput(tableData[tableName])
		else:
			if aggregate:
				#aggregate present
				aggFunt,aggAttr=getAggregateData(aggregate)
				if aggAttr not in keys:
					return printError("Invalid attribute "+aggAttr)
				if(aggFunt=="AVERAGE"):
					val=sum(tableData[tableName][aggAttr])/len(tableData[tableName][aggAttr])
					dataDict[aggregate]=[val]
					printOutput(dataDict)
				else:
					funct=aggregateFunctions[aggFunt]
					val=funct(tableData[tableName][aggAttr])
					dataDict[aggregate]=[val]
					printOutput(dataDict)
			else:
				if distinct:
					for attr in identifiers:
						if attr not in keys:
							return printError("Invalid attribute "+attr)
					
					length=len(tableData[tableName][identifiers[0]])
					mylist=[]
					for i in range(0,length):
						temp=[]
						for key in identifiers:
							temp.append(tableData[tableName][key][i])
						if temp not in mylist:
							mylist.append(temp)
					printRows([ table+"."+ident for ident in identifiers],mylist)

				else:
					for attr in identifiers:
						if attr not in keys:
							return printError("Invalid attribute "+attr)
						else:
							dataDict[tables[0]+"."+attr]=tableData[tableName][attr]
					printOutput(dataDict)
	################################### single table######################

	################################### multiple table ######################
	else:
		# tableName=tables[0]
		# keys=tableData.keys()
		dataDict={}
		if len(identifiers)==1 and identifiers[0]=="*":
			for tableName in tables:
				for attr in tableData[tableName].keys():
					dataDict[tableName+"."+attr]=tableData[tableName][attr]
			printOutput(dataDict)
		else:
			if aggregate:
				#aggregate present
				aggFunt,aggAttr=getAggregateData(aggregate)
				if aggAttr not in keys:
					return printError("Invalid attribute "+aggAttr)
				if(aggFunt=="AVERAGE"):
					val=sum(tableData[tableName][aggAttr])/len(tableData[tableName][aggAttr])
					dataDict[aggregate]=[val]
					printOutput(dataDict)
				else:
					funct=aggregateFunctions[aggFunt]
					val=funct(tableData[tableName][aggAttr])
					dataDict[aggregate]=[val]
					printOutput(dataDict)
			else:
				if distinct:
					for attr in identifiers:
						if attr not in keys:
							return printError("Invalid attribute "+attr)
					
					length=len(tableData[tableName][identifiers[0]])
					mylist=[]
					for i in range(0,length):
						temp=[]
						for key in identifiers:
							temp.append(tableData[tableName][key][i])
						if temp not in mylist:
							mylist.append(temp)
					printRows([ table+"."+ident for ident in identifiers],mylist)

				else:
					for attr in identifiers:
						if attr not in keys:
							return printError("Invalid attribute "+attr)
						else:
							dataDict[tables[0]+"."+attr]=tableData[tableName][attr]
					printOutput(dataDict)



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



def printOutput(dataDict):
	print("=================================================================")
	pprint.pprint(dataDict)
	keys=list(dataDict.keys())
	
	# data=[]
	# for key in keys:
	# 	data.append(dataDict[key])
	# # print(data)
	# lis = []
	# for lis in zip(i for i in data):
	# 	print(lis[0])
	# 	# print(temp[0][0])

	for key in keys:
		print("%15s  |"%(key),end=" ")
	print()
	print("=================================================================")
	length=len(dataDict[keys[0]])
	for i in range(0,length):
		for key in keys:
			print("%15s  |"%(dataDict[key][i]),end=" ")
			# print(dataDict[key][i],"   |", end=" ")
		print()
	print("=================================================================")



def validateQuery(tokens,tableDict):
	# pprint.pprint(tokens)
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

	# extractAggreagateAnd()

	if len(slicedTokens)-1==tablesindex:
		print("A")
		return True
	elif len(slicedTokens)==tablesindex+2:
		print("B")
		if slicedTokens[tablesindex+1].ttype is Whitespace:
			return True
		return printError("Invalid Query1")	
	elif len(slicedTokens)==tablesindex+3:
		print("C")
		if slicedTokens[tablesindex+1].ttype is Whitespace:
			if isinstance(slicedTokens[tablesindex+2],Where):
				whereClause=slicedTokens[tablesindex+2].value.upper()
				return True
		return printError("Invalid Query: did you mean 'where' ")	
	else:	
		print("D")
		return printError("Invalid Query")	

def extractFrom(tokens):
	# print(tokens)
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

def extractTable(tokens):
	return extractTableIdentifiers(tokens)

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
			# col=tableName.upper()+"."+line.strip().upper()
			col=line.strip().upper()
			tableDict[tableName].append(col)
		line = fp.readline()

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

if __name__ == "__main__":
	main()