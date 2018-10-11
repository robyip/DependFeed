import re

# with open("ipSomething.sql") as file: # file open
# with open("SQL/Procedures/ipAccountStatusNew.sql") as file: # file open
with open("SQL/Procedures/ipComponentStatus.sql") as file:
	data = file.read()

words = data.split()

print(words)
print()

# Create an empty dictionary
ProcAttributes = {}


# Get Schema
# if first word is USE the second is the schema

# Find Default Schema
if words[0].lower() == 'use':
	ProcAttributes['DefaultSchema'] = words[1]
	print('Default Procedure Schema is:', ProcAttributes['DefaultSchema'])
else:
	ProcAttributes['DefaultSchema'] = 'None'
	print('Default Procedure Schema Not found:', ProcAttributes['DefaultSchema'])

# Find Procedure name
for i, word in enumerate(words):
	if word.lower() == 'create':
		if words[i + 1].lower() == 'procedure':
			ProcAttributes['ProcedureName'] = words[i + 2]
			print('Procedure Name is:', ProcAttributes['ProcedureName'])
			break
		else:
			ProcAttributes['ProcedureName'] = 'None'
			print('Procedure Name Not found:', ProcAttributes['ProcedureName']) 

print('#### INSERT only');
# Find after an INSERT only Raw
# Not followed by INTO
# Excludes table name found with non alphanumeric characters and save them in an exception table
for i, word in enumerate(words):
	if word.lower() == 'insert' and words[i + 1].lower() != 'into':
		ProcAttributes['TableName'] = words[i + 1]
		if ProcAttributes['TableName'].isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'].isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 1] + ' - ' + words[i + 2] + ' - ' + words[i + 3]
			print('Table Name Not found:', ProcAttributes['TableName']) 


# Find after an INSERT INTO only Raw
# Excludes table name found with non alphanumeric characters and save them in an exception table
print('#### INSERT INTO');
for i, word in enumerate(words):
	if word.lower() == 'insert' and words[i + 1].lower() == 'into':
		ProcAttributes['TableName'] = words[i + 2] 
		if ProcAttributes['TableName'] .isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'] .isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 2] + ' - ' + words[i + 3] + ' - ' + words[i + 4]	
			print('Table Name Not found:', ProcAttributes['TableName']) 



# FROM 
# not a DELETE FROM
print('#### FROM');
for i, word in enumerate(words):
	if word.lower() == 'from' and words[i - 1].lower() != 'delete':
		ProcAttributes['TableName'] = words[i + 1] 
		if ProcAttributes['TableName'].isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'].isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 1] + ' - ' + words[i + 2] + ' - ' + words[i + 3]	
			print('Table Name Not found:', ProcAttributes['TableName']) 


# INNER JOIN
print('#### INNER JOIN');
for i, word in enumerate(words):
	if word.lower() == 'inner' and words[i + 1].lower() == 'join':
		ProcAttributes['TableName'] = words[i + 2] 
		if ProcAttributes['TableName'] .isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'] .isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 2] + ' - ' + words[i + 3] + ' - ' + words[i + 4]	
			print('Table Name Not found:', ProcAttributes['TableName']) 

# LEFT JOIN
print('#### LEFT JOIN');
for i, word in enumerate(words):
	if word.lower() == 'left' and words[i + 1].lower() == 'join':
		ProcAttributes['TableName'] = words[i + 2] 
		if ProcAttributes['TableName'] .isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'] .isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 2] + ' - ' + words[i + 3] + ' - ' + words[i + 4]	
			print('Table Name Not found:', ProcAttributes['TableName']) 

# RIGHT JOIN
print('#### RIGHT JOIN');
for i, word in enumerate(words):
	if word.lower() == 'right' and words[i + 1].lower() == 'join':
		ProcAttributes['TableName'] = words[i + 2] 
		if ProcAttributes['TableName'] .isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'] .isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 2] + ' - ' + words[i + 3] + ' - ' + words[i + 4]	
			print('Table Name Not found:', ProcAttributes['TableName']) 


# LEFT OUTER JOIN
print('#### LEFT OUTER JOIN');
for i, word in enumerate(words):
	if word.lower() == 'left' and words[i + 1].lower() == 'outer' and words[i + 2].lower() == 'join':
		ProcAttributes['TableName'] = words[i + 3] 
		if ProcAttributes['TableName'] .isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'] .isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 3] + ' - ' + words[i + 4] + ' - ' + words[i + 5]	
			print('Table Name Not found:', ProcAttributes['TableName'])

# RIGHT OUTER JOIN
print('#### RIGHT OUTER JOIN');
for i, word in enumerate(words):
	if word.lower() == 'right' and words[i + 1].lower() == 'outer' and words[i + 2].lower() == 'join':
		ProcAttributes['TableName'] = words[i + 3] 
		if ProcAttributes['TableName'] .isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'] .isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 3] + ' - ' + words[i + 4] + ' - ' + words[i + 5]	
			print('Table Name Not found:', ProcAttributes['TableName'])

# none of the above join type exception
	# TBD

# DELETE FROM
print('#### DELETE FROM')
for i, word in enumerate(words):
	if word.lower() == 'delete' and words[i + 1].lower() == 'from':
		ProcAttributes['TableName'] = words[i + 2] 
		if ProcAttributes['TableName'] .isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'] .isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 2] + ' - ' + words[i + 3] + ' - ' + words[i + 4]	
			print('Table Name Not found:', ProcAttributes['TableName'])  

# DELETE only
for i, word in enumerate(words):
	if word.lower() == 'delete' and words[i + 1].lower() != 'from':
		ProcAttributes['TableName'] = words[i + 1]
		if ProcAttributes['TableName'].isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'].isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 1] + ' - ' + words[i + 2] + ' - ' + words[i + 3]
			print('Table Name Not found:', ProcAttributes['TableName'])

# TRUNCATE TABLE
print('#### TRUNCATE TABLE')
for i, word in enumerate(words):
	if word.lower() == 'truncate' and words[i + 1].lower() == 'table':
		ProcAttributes['TableName'] = words[i + 2] 
		if ProcAttributes['TableName'] .isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'] .isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 2] + ' - ' + words[i + 3] + ' - ' + words[i + 4]	
			print('Table Name Not found:', ProcAttributes['TableName'])  

# UPDATE - which table is the updated table?
print('#### UPDATE');
for i, word in enumerate(words):
	if word.lower() == 'update':
		ProcAttributes['TableName'] = words[i + 1] 
		if ProcAttributes['TableName'].isalnum():
			print('Table Name is:', ProcAttributes['TableName'])
		# Save as an exception for review 
		elif not(ProcAttributes['TableName'].isalnum()):
			print('Table Name is an exception :', ProcAttributes['TableName'])
		else:
			ProcAttributes['TableName'] = words[i + 1] + ' - ' + words[i + 2] + ' - ' + words[i + 3]	
			print('Table Name Not found:', ProcAttributes['TableName']) 


# print(ProcAttributes)
# print(words)
