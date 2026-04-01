/*
================================================================================
Create Database and Schemas
================================================================================
Script Purpose : 
This script sets up three schemas within the database:'bronze','silver' and 'gold'.

*/


-- create database 'DataWarehouse'

use master;

create database DataWarehouse;
use Datawarehouse;

create schema bronze;
go
create schema silver;
go
create schema gold;
go
