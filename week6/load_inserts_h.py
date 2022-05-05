# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import psycopg2.extras
import argparse
import re
import csv

DBname = "postgres"
DBuser = "postgres"
DBpwd = "password"
TableName = 'censusdata'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = True  # indicates whether the DB table should be (re)-created

def row2vals(row):
	for key in row:
		if not row[key]:
			row[key] = 0  # ENHANCE: handle the null vals
		row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals

	ret = [
	   row['CensusTract'],
	   row['State'],
	   row['County'],
	   row['TotalPop'],
	   row['Men'],
	   row['Women'],
	   row['Hispanic'],
	   row['White'],
	   row['Black'],
	   row['Native'],
	   row['Asian'],
	   row['Pacific'],
	   row['Citizen'],
	   row['Income'],
	   row['IncomeErr'],
	   row['IncomePerCap'],
	   row['IncomePerCapErr'],
	   row['Poverty'],
	   row['ChildPoverty'],
	   row['Professional'],
	   row['Service'],
	   row['Office'],
	   row['Construction'],
	   row['Production'],
	   row['Drive'],
	   row['Carpool'],
	   row['Transit'],
	   row['Walk'],
	   row['OtherTransp'],
	   row['WorkAtHome'],
	   row['MeanCommute'],
	   row['Employed'],
	   row['PrivateWork'],
	   row['PublicWork'],
	   row['SelfEmployed'],
	   row['FamilyWork'],
	   row['Unemployment'],
	]

	return ret


def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

# read the input data file into a list of row strings
def readdata(fname):
	print(f"readdata: reading from File: {fname}")
	with open(fname, mode="r") as fil:
		dr = csv.DictReader(fil)
		
		rowlist = []
		for row in dr:
			rowlist.append(row)

	return rowlist

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
	cmdlist = []
	for row in rowlist:
		valstr = row2vals(row)
		cmdlist.append(valstr)
	return cmdlist

# connect to the database
def dbconnect():
	connection = psycopg2.connect(
		host="localhost",
		database=DBname,
		user=DBuser,
		password=DBpwd,
	)
	connection.autocommit = True
	return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
			DROP TABLE IF EXISTS {TableName};
			CREATE TABLE {TableName} (
				CensusTract         NUMERIC,
				State               TEXT,
				County              TEXT,
				TotalPop            INTEGER,
				Men                 INTEGER,
				Women               INTEGER,
				Hispanic            DECIMAL,
				White               DECIMAL,
				Black               DECIMAL,
				Native              DECIMAL,
				Asian               DECIMAL,
				Pacific             DECIMAL,
				Citizen             DECIMAL,
				Income              DECIMAL,
				IncomeErr           DECIMAL,
				IncomePerCap        DECIMAL,
				IncomePerCapErr     DECIMAL,
				Poverty             DECIMAL,
				ChildPoverty        DECIMAL,
				Professional        DECIMAL,
				Service             DECIMAL,
				Office              DECIMAL,
				Construction        DECIMAL,
				Production          DECIMAL,
				Drive               DECIMAL,
				Carpool             DECIMAL,
				Transit             DECIMAL,
				Walk                DECIMAL,
				OtherTransp         DECIMAL,
				WorkAtHome          DECIMAL,
				MeanCommute         DECIMAL,
				Employed            INTEGER,
				PrivateWork         DECIMAL,
				PublicWork          DECIMAL,
				SelfEmployed        DECIMAL,
				FamilyWork          DECIMAL,
				Unemployment        DECIMAL
			);	
			ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
			CREATE INDEX idx_{TableName}_State ON {TableName}(State);
		""")

		print(f"Created {TableName}")

def load(conn, icmdlist):

	with conn.cursor() as cursor:
		print(f"Loading {len(icmdlist)} rows")
		start = time.perf_counter()
		
		psycopg2.extras.execute_batch(cursor, "INSERT INTO {table} (CensusTract, State, County, TotalPop, Men, Women, Hispanic, White, Black, Native, Asian, Pacific, Citizen, Income, IncomeErr, IncomePerCap, IncomePerCapErr, Poverty, ChildPoverty, Professional, Service, Office, Construction, Production, Drive, Carpool, Transit, Walk, OtherTransp, WorkAtHome, MeanCommute, Employed, PrivateWork, PublicWork, SelfEmployed, FamilyWork, Unemployment) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);".format(table=TableName), icmdlist)

		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
	initialize()
	conn = dbconnect()
	rlis = readdata(Datafile)
	cmdlist = getSQLcmnds(rlis)

	if CreateDB:
		createTable(conn)

	load(conn, cmdlist)


if __name__ == "__main__":
	main()



