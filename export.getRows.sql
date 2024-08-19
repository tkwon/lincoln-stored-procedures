create procedure [export].[sp_getData]
	@flags varchar(255)
	,@parameters nvarchar(max)
	,@columns nvarchar(max)
	,@group_by_selection varchar(100)
	,@id varchar(20)

as

begin

declare @sql nvarchar(max)
declare @role varchar(20)
declare @rlsWhereClause varchar(1000)


----- Get parameters and save each parameters value in separate temp table

drop table if exists #parametersJson

select @parameters as [parametrs] into #parametersJson


drop table if exists #parameters

select
	 w.[name]
	,w.[value]
	,row_number() over(order by [name]) as [rowN]
into #parameters
from (
	select 
		o.[name]
		,o.[value] as [value]
	from #parametersJson f
	cross apply openjson([parametrs])
	with (
		[name] varchar(2000)
		,[value] nvarchar(max) as json
	) o
) w

---- Reporting Period -----

drop table if exists #reportingPeriodEndDate

select 
	[name]
	,cast(ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as date) as [date]
into #reportingPeriodEndDate
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'Reporting_Period_End_Date'


declare @reportingPeriodWhereClause varchar(2000)

if (select count(*) from #reportingPeriodEndDate) > 0
	set @reportingPeriodWhereClause = ' and [Reporting_Period_End_Date] in (select [date] from #reportingPeriodEndDate)'
else
	set @reportingPeriodWhereClause = ''


---- TPA -----

drop table if exists #tpa

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [TPA]
into #tpa
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'TPA_Name'


if (select count(*) from #tpa) > 0
	set @role = 'TPA'
if (select count(*) from #tpa) > 0
	set @rlsWhereClause = 'select [TPA] from #tpa'
	
---- Underwriter -----

drop table if exists #underwriter

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [Underwriter]
into #underwriter
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'Underwriter_Name'


if (select count(*) from #underwriter) > 0
	set @role = 'Underwriter'
if (select count(*) from #underwriter) > 0
	set @rlsWhereClause = 'select [Underwriter] from #underwriter'


---- Coverholder ----

drop table if exists #coverholder

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [Coverholder]
into #coverholder
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'Coverholder_Name'


if (select count(*) from #coverholder) > 0
	set @role = 'Coverholder'
if (select count(*) from #coverholder) > 0
	set @rlsWhereClause = 'select [Coverholder] from #coverholder'

---- UMR -----

drop table if exists #umr

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [umr]
into #umr
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'Unique_Market_Reference_UMR'


declare @umrWhereClause varchar(100)

if (select count(*) from #umr) > 0
	set @umrWhereClause = ' where [UMR] in (select [umr] from #umr)'
else
	set @umrWhereClause = ''


---- Flags ----

drop table if exists #flagsJson

select @flags as [flags] into #flagsJson

drop table if exists #flags

select
	 w.[name]
	,w.[value]
	,row_number() over(order by [name]) as [rowN]
into #flags
from (
	select 
		o.[name]
		,o.[value]
	from #flagsJson f
	cross apply openjson([flags])
	with (
		[name] varchar(50)
		,[value] varchar(50)
	) o
) w


declare @i tinyint = 1
declare @rows tinyint = (select max([rowN]) from #flags)
declare @flagsWhereClause varchar(255) = 'where ('
declare @currentName varchar(25)
declare @currentValue char(1)

while @i <= @rows
	begin
		set @currentName = (select [name] from #flags where [rowN] = @i)
		set @currentValue = (select [value] from #flags where [rowN] = @i)

		set @flagsWhereClause = @flagsWhereClause + @currentName + ' = ' + @currentValue

		if @i <> @rows
			begin
				set @flagsWhereClause = @flagsWhereClause + ' or '
			end

		set @i = @i + 1
	end

set @flagsWhereClause = @flagsWhereClause + ')'


---- Columns -----

set @columns = replace(replace(@columns, '",', '],'), '"', '[')
set @columns = substring(@columns, 0, len(@columns)) + ']'

-- INVALID COLUMNS:
--Invalid column name 'Date_of_Loss_To'.
--Invalid column name 'Reg_No_of_Vehicle_etc'.
--Invalid column name 'Ceded_Reinsurance'.
--Invalid column name 'Plan'.
--Invalid column name 'Patient_Name'.
--Invalid column name 'Treatment_Type'.
--Invalid column name 'Country_of_Treatment'.
--Invalid column name 'Date_of_Treatment'.
--Invalid column name 'BDX_Key'.

-- Duplicated: Date_Coverage_Confirmed 



---- 1) find the matching Group to the value -----

drop table if exists ##rlsIds

set @sql = 'select * into ##rlsIds
			from [dbo].[rls_filterset_rls_' + @role + ']
			where [parent_id] in (
				select [id] from [dbo].[rls_filterset_rls_' + @role + '] where ' + @role + ' in (' + @rlsWhereClause + ') and [parent_id] is null
	)
'
exec(@sql)


--select * from ##rlsIds

---- 2) Get UMRs for specified group ----
drop table if exists ##umr

set @sql = 'select distinct d.[UMR]
			into ##umr
			from [dbo].[rls_filterset_umr_' + @role + '] d
			join ##rlsIds r on r.[id] = d.[RLS_'+ @role + '_id] '
			+ @umrWhereClause

exec(@sql)


--select * from ##umr
--select * from #activeUmr

---- 3) Get active umrs ----
drop table if exists #activeUmr

select distinct
	g.[umr_rc_cc_sn]
into #activeUmr
from [dbo].[rls_filterset_globalumr] g
join ##umr u on u.[UMR] = g.[umr]
where g.[bdx_status] = 1



---- 4) Get list of underwriters and brokers for grouping ----

drop table if exists #underwriters

select distinct 
	g.[umr]
	,rls.[underwriter] as [Underwriter]
into #underwriters
from [dbo].[rls_filterset_globalumr] g
join ##umr u on u.[UMR] = g.[umr]
left join [dbo].[rls_filterset_umr_underwriter] rlsumr on rlsumr.[umr] = g.[umr]
join [dbo].[rls_filterset_rls_underwriter] rls on rls.[id] = rlsumr.[RLS_Underwriter_id]

drop table if exists #brokers

select distinct 
	g.[umr]
	,rls.[broker] as [Broker]
into #brokers
from [dbo].[rls_filterset_globalumr] g
join ##umr u on u.[UMR] = g.[umr]
left join [dbo].[rls_filterset_umr_broker] rlsumr on rlsumr.[umr] = g.[umr]
join [dbo].[rls_filterset_rls_broker] rls on rls.[id] = rlsumr.[RLS_Broker_id]


--select * from #activeUmr

---- 5) Get rows from datarows table (based on user's columns and flags selection) -----

set @sql = 'drop table if exists [export].[getData_' + @id + ']'
exec(@sql)

set @sql = 'select u.[Underwriter], b.[Broker], d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'') as [umr_rc_cc_sn],' 
			+ @columns + '
			into [export].[getData_' + @id + ']
			from [dbo].[rig_datarows] d
			join #activeUmr a on a.[umr_rc_cc_sn] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')
			join [dbo].[rig_dataintegrityrules] dir on dir.[data_row_id] = d.[id]
			left join #underwriters u on u.[umr] = d.[Unique_Market_Reference_UMR]
			left join #brokers b on b.[umr] = d.[Unique_Market_Reference_UMR]'
			+ @flagsWhereClause
			+ @reportingPeriodWhereClause 
			+ ' order by [umr_rc_cc_sn]'

--print cast(substring(@sql, 1, 16000) as ntext )
--print cast(substring(@sql, 16001, 32000) as ntext )

exec(@sql)

---- create index on new table ----

declare @createindexsql varchar(255)

set @createindexsql = 'create nonclustered index ix_nc_exportGetData_' + @id + ' on [export].[getData_' + @id + '] ([Reporting_Period_End_Date], [umr_rc_cc_sn], [' + @group_by_selection + '])'

exec(@createindexsql)

--select top 1000 * from ##tempResult

---- 6) Grouping data ----

drop table if exists ##coverholderGroup

set @sql = 'select distinct
	e.[Coverholder_Name]
	,case 
		when c.[parent_id] is null then coalesce(c.[coverholder],''Missing'')
		else coalesce(cc.[coverholder],''Missing'')
	end as [Coverholder_final]
into ##coverholderGroup
from [export].[getData_' + @id + '] e
left join [dbo].[rls_filterset_rls_coverholder] c on c.[coverholder] = e.[Coverholder_Name]
left join [dbo].[rls_filterset_rls_coverholder] cc on cc.[id] = c.[parent_id]'

exec(@sql)

set @sql = 'drop table if exists [export].[getData_' + @id + '_grouped]'
exec(@sql)

set @sql = 'select
	[Reporting_Period_End_Date_Folder]
	,[' + @group_by_selection + '_Folder]
	,[umr_rc_cc_sn_File]
	,[RowsNumber]
	,''select * from [export].[getData_' + @id + '] where [Reporting_Period_End_Date] = '''''' + convert(varchar(10),[Reporting_Period_End_Date]) + '''''' and [' + @group_by_selection + '] = '''''' + [' + @group_by_selection + '_Folder] + '''''' and [umr_rc_cc_sn] = '''''' + [umr_rc_cc_sn] + '''''''' as [query]
into [export].[getData_' + @id + '_grouped]
from (
	select distinct
		''RptDate-'' + convert(varchar(10), [Reporting_Period_End_Date], 32) as [Reporting_Period_End_Date_Folder]
		,[' + @group_by_selection + '] as [' + @group_by_selection + '_Folder]
		,cg.[Coverholder_final] + ''-'' + replace([umr_rc_cc_sn],''_'',''-'') + ''-'' + ''(RptDate-'' + convert(varchar(10), [Reporting_Period_End_Date], 32) + '')'' as [umr_rc_cc_sn_File]
		,count(*) as [RowsNumber]
		,[umr_rc_cc_sn]
		,[Reporting_Period_End_Date]
	from [export].[getData_' + @id + '] e
	left join ##coverholderGroup cg on cg.[Coverholder_Name] = e.[Coverholder_Name]
	group by [Reporting_Period_End_Date],[' + @group_by_selection + '] ,[umr_rc_cc_sn],cg.[Coverholder_final]
) w
order by [Reporting_Period_End_Date_Folder],[' + @group_by_selection + '_Folder] ,[umr_rc_cc_sn_File]'



exec(@sql)

end
GO


