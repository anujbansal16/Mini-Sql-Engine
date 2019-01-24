import sys
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
aggregateFunctions=["SUM", "AVERAGE", "MAX","MIN"]

def printError(s):
	print(s)
	return False

def main():
	if len(sys.argv)>1:
		#read meta data file
		readMetaData()
		# loadTablest()
		#read command line
		# print(tableDict)
		sqlQueryStmt=parser.parse(sys.argv[1])[0]
		if validateQuery(sqlQueryStmt.tokens,tableDict):
			print("Success")
			executeQuery()
		else:
			printError("Something goes wrong")
		# print(tables,identifiers,whereClause,distinct,aggregate)

	else:
		print("Provide sql query as argument")

def executeQuery():
	print(tables,identifiers,whereClause,distinct,aggregate)
	print(tableDict)
	for table in tables:
		if table not in tableDict.keys():
			return printError("Error: %s table doesn't exist "%(table))
	# if len(tables)==1:
		



def validateQuery(tokens,tableDict):
	pprint.pprint(tokens)
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
			tables.append(tokenitem.value)
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
			tableName=line.strip().upper()
			tableDict[tableName]=[]
			table=False
		else:
			tableDict[tableName].append(tableName+"."+line.strip().upper())
		line = fp.readline()

def loadTables():
	for table in tableDict:
		pd

if __name__ == "__main__":
	main()