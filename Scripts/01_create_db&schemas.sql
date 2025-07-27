/*********************************************************************************************\
===========================================
create database and schema
==========================================
SCRIPT PURPOSE: Create DataWareHouse Database and Bronze/Silver/Gold Schemas additionally creates three schemas
'bronze','silver','gold'
WARNING: This DROPS the DataWareHouse database if it exists. All data will be lost.USE WITH CAUTION!
*********************************************************************************************/
--create database and schemas
use master;
--check for existing database with same name
if exists (select 1 from sys.databases where name='DataWareHouse')
begin
   alter database DataWareHouse set single_user with rollback immediate;
   drop database DataWareHouse;
end;
go

create database DataWareHouse;
go
use DataWareHouse;
go
--create shemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go
