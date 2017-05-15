# 1. Set the permission to allow sqoop get access to Accounts and Contacts tables in mysql

GRANT ALL PRIVILEGES ON homework4.* to ‘’@‘localhost’;


# 2. Use Sqoop to import two tables into Hive

sqoop import-all-tables --connect jdbc:mysql://localhost/homework4 --driver com.mysql.jdbc.Driver --hive-import --hive-database homework4 -m 1


# 3. Create two encrypted tables in hive

**Encrypted_accounts table**

CREATE TABLE encrypted_accounts (
  id BIGINT,  
  name VARCHAR(50),  
  phone CHAR(12)  
);

**Encrypted_contacts table**

CREATE TABLE encrypted_contacts (  
  id BIGINT,  
  account_id BIGINT,  
  first_name VARCHAR(50),  
  last_name VARCHAR(50),  
  phone CHAR(12),  
  email VARCHAR(50)  
);


# 4. Add UDF JAR file in Hive

ADD JAR PIIWatcher.jar;


# 5. Create a temporary function using this JAR

CREATE TEMPORARY function PIIWatcher as ‘PIIWatcher’;


# 6. Insert Overwrite into two encrypted tables using Hive UDF

**Encrypted_accounts table**

INSERT OVERWRITE TABLE encrypted_accounts  
SELECT id, name, PIIWatcher(phone)  
FROM accounts;

**Encrypted_contacts table**

INSERT OVERWRITE TABLE encrypted_contacts  
SELECT id, account_id, first_name, last_name, PIIWatcher(phone), PIIWatcher email)  
FROM contacts;


# 7. Create two encrypted table in mysql:

**Encrypted_accounts table**

CREATE TABLE encrypted_accounts (  
  id INTEGER,  
  name VARCHAR(50),  
  phone CHAR(12)  
);

**Encrypted_contacts table**

CREATE TABLE encrypted_contacts (  
  id INTEGER,  
  account_id INTEGER,  
  first_name VARCHAR(50),  
  last_name VARCHAR(50),  
  phone CHAR(12),  
  email VARCHAR(50)  
);


# 8. Use sqoop to export two encrypted tables from hive to mySQL database

**Encrypted_accounts table**

sqoop export --connect jdbc:mysql://localhost/homework4 --driver com.mysql.jdbc.Driver --table encrypted_accounts --export-dir /apps/hive/warehouse/homework4.db/encrypted_accounts --input-fields-terminated-by '\0001' -m 1

**Encrypted_contacts table**

sqoop export --connect jdbc:mysql://localhost/homework4 --driver com.mysql.jdbc.Driver --table encrypted_contacts --export-dir /apps/hive/warehouse/homework4.db/encrypted_contacts --input-fields-terminated-by '\0001' -m 1