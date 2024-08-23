create or alter procedure [export].[sp_getData]
	@flags varchar(255)
	,@parameters nvarchar(max)
	,@columns nvarchar(max)
	,@group_by_selection varchar(100)
	,@id varchar(20)
	,@group_by varchar(255)

as

begin

declare @sql nvarchar(max)
declare @role varchar(20) = ''
declare @rlsWhereClause varchar(1000)

---- Coverholder case -----
set @group_by_selection = case when @group_by_selection = 'Coverholder' then 'Coverholder_Name' else @group_by_selection end

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


declare @umrWhereClause nvarchar(max)

if (select count(*) from #umr) > 0
	set @umrWhereClause = ' where [UMR] in (select [umr] from #umr)'
else
	set @umrWhereClause = ''


---- Flags and Group_by ----

declare @group_by_columns varchar(255)
select @group_by_columns = string_agg(value, ', ') from openjson(@group_by)

declare @umr_flags_join varchar(255)

select @umr_flags_join = 'join #umr_flags uf on ' + string_agg(
    'uf.' + value + ' = d.' + value, 
    ' and '
	)
	from openjson(@group_by)

if @flags = '[]'
	set @umr_flags_join = ''



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
declare @flagsWhereClause varchar(255) = 'where uf.[Flag] in ('
declare @currentName varchar(25)
--declare @currentValue char(1)

while @i <= @rows
	begin
		set @currentName = (select [name] from #flags where [rowN] = @i and [value] = 1)
		--set @currentValue = (select [value] from #flags where [rowN] = @i)

		set @flagsWhereClause = @flagsWhereClause + '''' + @currentName + ''''

		if @i <> @rows
			begin
				set @flagsWhereClause = @flagsWhereClause + ','
			end

		set @i = @i + 1
	end

set @flagsWhereClause = @flagsWhereClause + ')'

if @flags = '[]'
	set @flagsWhereClause = ' where 1=1'

---- Columns -----

set @columns = replace(replace(@columns, '",', '],'), '"', 'd.[')
set @columns = substring(@columns, 0, len(@columns)-2) + ']'

declare @columns_index nvarchar(max) = case when @group_by_selection = 'Coverholder_Name' then replace(replace(replace(@columns, 'd.', ''),'[Reporting_Period_End_Date],',''),'[Coverholder_Name],','') else replace(replace(@columns, 'd.', ''),'[Reporting_Period_End_Date],','') end

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

if @role <> ''
begin

	drop table if exists ##rlsIds
	
	set @sql = 'select * into ##rlsIds
				from [dbo].[rls_filterset_rls_' + @role + ']
				where [parent_id] in (
					select [id] from [dbo].[rls_filterset_rls_' + @role + '] where ' + @role + ' in (' + @rlsWhereClause + ') and [parent_id] is null
		)
	'
	exec(@sql)

end
	
declare @tpaWhereClause varchar(255)

if @role = 'TPA'
	set @tpaWhereClause = ' and [TPA_Name] in (select [TPA] from ##rlsIds)'
else set @tpaWhereClause = ''

declare @coverholderWhereClause varchar(255)

if @role = 'Coverholder'
	set @coverholderWhereClause = ' and [Coverholder_Name] in (select [Coverholder] from ##rlsIds)'
else set @coverholderWhereClause = ''

declare @underwriterWhereClause varchar(255)

if @role = 'Underwriter'
	set @underwriterWhereClause = ' and u.[Underwriter] in (select [underwriter] from ##rlsIds)'
else set @underwriterWhereClause = ''


--select * from ##rlsIds

---- 2) Get UMRs for specified group ----
drop table if exists ##umr

create table ##umr (
	[UMR] nvarchar(128) null
	,[Risk_Code] nvarchar(64) null
	,[Section_No] nvarchar(64) null
	,[umr_rc_sn] nvarchar(256) null
)

if @role <> ''
begin

	set @sql = 'insert ##umr ([UMR],[Risk_Code],[Section_No],[umr_rc_sn])
				select distinct d.[UMR], coalesce(nullif(d.[Risk_Code],''''), ''0'') as [Risk_Code], coalesce(nullif(d.[Section_No],''''), ''0'') as [Section_No]
					,d.[UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'') as [umr_rc_sn]
				from [dbo].[rls_filterset_umr_' + @role + '] d
				join ##rlsIds r on r.[id] = d.[RLS_'+ @role + '_id] '
				+ @umrWhereClause
	
	exec(@sql)

end

if @role = ''
begin

	set @sql = 'insert ##umr ([umr_rc_sn])
				select distinct d.[umr_rc_sn]
				from [dbo].[rls_filterset_globalumr] d'
				+ @umrWhereClause
	
	exec(@sql)

end


--select * from ##umr
--select * from #activeUmr

---- 3) Get active umrs ----
drop table if exists #activeUmr

select distinct
	g.[umr_rc_cc_sn]
into #activeUmr
from [dbo].[rls_filterset_globalumr] g
join ##umr u on u.[umr_rc_sn] = g.[umr_rc_sn]
where g.[bdx_status] = 1


--select * from #underwriters

---- 4) Get list of underwriters and brokers for grouping ----

drop table if exists #underwriters

select distinct 
	g.[umr]
	,rls.[underwriter] as [Underwriter]
	,rlsumr.[UMR_Risk_Section]
into #underwriters
from [dbo].[rls_filterset_globalumr] g
join ##umr u on u.[umr_rc_sn] = g.[umr_rc_sn]
left join [dbo].[rls_filterset_umr_underwriter] rlsumr on rlsumr.[umr] = g.[umr] and coalesce(nullif(rlsumr.[Risk_Code],''), '0') = coalesce(nullif(g.[Risk_Code],''), '0') and coalesce(nullif(rlsumr.[Section_No],''), '0') = coalesce(nullif(g.[Section_No],''), '0')
join [dbo].[rls_filterset_rls_underwriter] rls on rls.[id] = rlsumr.[RLS_Underwriter_id]

drop table if exists #brokers

select distinct 
	g.[umr]
	,rls.[broker] as [Broker]
	,rlsumr.[UMR_Risk_Section]
into #brokers
from [dbo].[rls_filterset_globalumr] g
join ##umr u on u.[umr_rc_sn] = g.[umr_rc_sn]
left join [dbo].[rls_filterset_umr_broker] rlsumr on rlsumr.[umr] = g.[umr] and coalesce(nullif(rlsumr.[Risk_Code],''), '0') = coalesce(nullif(g.[Risk_Code],''), '0') and coalesce(nullif(rlsumr.[Section_No],''), '0') = coalesce(nullif(g.[Section_No],''), '0')
join [dbo].[rls_filterset_rls_broker] rls on rls.[id] = rlsumr.[RLS_Broker_id]


---- Assigning flags to UMRs -----

drop table if exists ##temp
set @sql = 'select ' + @group_by_columns + ' 
				,[Critical_Error_Flag]
				,[Non_Critical_Flag]
				,row_number() over(partition by ' + @group_by_columns + ' order by ' + @group_by_columns + ', [Critical_Error_Flag] desc) as [rowN]
			into ##temp
			from (	
				select distinct ' + @group_by_columns + '
						,dir.[Critical_Error_Flag]
						,dir.[Non_Critical_Flag]
				from [dbo].[rig_datarows] d
				join #activeUmr a on a.[umr_rc_cc_sn] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')
				join [dbo].[rig_dataintegrityrules] dir on dir.[data_row_id] = d.[id] '
				+ @reportingPeriodWhereClause +
				' ) w'

exec(@sql)

drop table if exists #umr_flags

select *
	,case
		when [Critical_Error_Flag] = 1 then 'Critical_Error_Flag'
		when [Critical_Error_Flag] = 0 and [Non_Critical_Flag] = 1 then 'Non_Critical_Flag'
		else 'No Flag'
	 end as [Flag]
into #umr_flags
from ##temp
where [rowN] = 1



--select * from #umr_flags
--select * from #activeUmr

---- 5) Get rows from datarows table (based on user's columns and flags selection) -----

set @sql = 'drop table if exists [export].[getData_' + @id + ']'
exec(@sql)

set @sql = 'select u.[Underwriter], b.[Broker], d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'') as [umr_rc_cc_sn],' 
			+ @columns + '
			into [export].[getData_' + @id + ']
			from [dbo].[rig_datarows] d
			join #activeUmr a on a.[umr_rc_cc_sn] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')'
			+ @umr_flags_join + '
			left join #underwriters u on u.[UMR_Risk_Section] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')
			left join #brokers b on b.[UMR_Risk_Section] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')'
			+ @flagsWhereClause
			+ @reportingPeriodWhereClause 
			+ @tpaWhereClause
			+ @coverholderWhereClause
			+ @underwriterWhereClause

--print cast(substring(@sql, 1, 16000) as ntext )
--print cast(substring(@sql, 16001, 32000) as ntext )

exec(@sql)

---- create index on new table ----

declare @createindexsql nvarchar(max)

set @createindexsql = 'create nonclustered index ix_nc_exportGetData_' + @id + ' on [export].[getData_' + @id + '] ([Reporting_Period_End_Date], [umr_rc_cc_sn], [' + @group_by_selection + ']) include (' + @columns_index + ')'

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

declare @group_by_selection_folder varchar(25) =  case when @group_by_selection = 'Coverholder_Name' then 'Coverholder' else @group_by_selection end
declare @coverholder_prefix varchar(5) = case when @group_by_selection = 'Coverholder_Name' then 'cg.' else '' end
set @group_by_selection = case when @group_by_selection = 'Coverholder_Name' then 'Coverholder_final' else @group_by_selection end

set @sql = 'select
	[Reporting_Period_End_Date_Folder]
	,[' + @group_by_selection_folder + '_Folder]
	,[umr_rc_cc_sn_File]
	,[RowsNumber]
	,''select ' + @columns + ' from [export].[getData_' + @id + '] d where [Reporting_Period_End_Date] = '''''' + convert(varchar(10),[Reporting_Period_End_Date]) + '''''' and coalesce([' + @group_by_selection + '],'''''''') = '''''' + [' + @group_by_selection_folder + '_Folder] + '''''' and [umr_rc_cc_sn] = '''''' + [umr_rc_cc_sn] + '''''''' as [query]
into [export].[getData_' + @id + '_grouped]
from (
	select distinct
		''RptDate-'' + convert(varchar(10), [Reporting_Period_End_Date], 32) as [Reporting_Period_End_Date_Folder]
		,coalesce('+ @coverholder_prefix +'[' + @group_by_selection + '],'''') as [' + @group_by_selection_folder + '_Folder]
		,cg.[Coverholder_final] + ''-'' + replace([umr_rc_cc_sn],''_'',''-'') + ''-'' + ''(RptDate-'' + convert(varchar(10), [Reporting_Period_End_Date], 32) + '')'' as [umr_rc_cc_sn_File]
		,count(*) as [RowsNumber]
		,[umr_rc_cc_sn]
		,[Reporting_Period_End_Date]
	from [export].[getData_' + @id + '] e
	left join ##coverholderGroup cg on cg.[Coverholder_Name] = e.[Coverholder_Name]
	group by [Reporting_Period_End_Date],' + @coverholder_prefix + '[' + @group_by_selection + '] ,[umr_rc_cc_sn],cg.[Coverholder_final]
) w
order by [Reporting_Period_End_Date_Folder],[' + @group_by_selection_folder + '_Folder] ,[umr_rc_cc_sn_File]'

exec(@sql)


end


