if not exists (select * from sys.schemas where name = 'export')
	exec('create schema [export] authorization [dbo]')