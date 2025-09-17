create or alter procedure [export].[sp_FiltersSelect]
	@parameters nvarchar(max)
	,@role varchar(20)
	,@role_value varchar(255)
	,@umr_criteria varchar(20)
	,@uw_option varchar(10) = 'Lead'

as

begin

declare @sql nvarchar(max)

---- Role handling -----

drop table if exists ##roleValues 

select
	[value]
into ##roleValues
from string_split(@role_value, ',')

--select * from ##roleValues

declare @roleValueWhereClause varchar(255) = ''
declare @roleUmrJoin varchar(255) = ''

if @role <> 'admin'

begin

declare @lead_condition varchar(500) = case
							when @role = 'Underwriter' and @uw_option = 'Lead' then ' and umr.[lead] = 1'
							when @role = 'Underwriter' and @uw_option = 'Follow' then ' and umr.[lead] = 0'
							when @role = 'TPA' then ' and exists (
												select 1 
												from [dbo].[rls_filterset_umr_underwriter] u 
												where u.[UMR] = umr.[UMR] 
												and coalesce(nullif(u.[Risk_Code],''''), ''0'') = coalesce(nullif(umr.[Risk_Code],''''), ''0'')
												and coalesce(nullif(u.[Section_No],''''), ''0'') = coalesce(nullif(umr.[Section_No],''''), ''0'')
												and u.[lead] = 1)'
							else ''
						end


	drop table if exists ##roleFiltered
	
	set @sql = 'select '
		+ @role + '
	into ##roleFiltered
	from (
		select 
			t1.[' + @role + ']
			,row_number() over(partition by t2.[' + @role + '] order by t2.[' + @role + ']) as [rowN]
		from [rls_filterset_umr_' + @role + '] umr
		join [rls_filterset_rls_' + @role + '] t1 on umr.[RLS_' + @role + '_id] = t1.[id]
		join [rls_filterset_rls_' + @role + '] t2 on t1.[parent_id] = t2.[id]
		where t2.[' + @role + '] in (select [value] from ##roleValues) 
	) w
	where [rowN] = 1'
	
	exec(@sql)

	set @roleValueWhereClause = ' where d.[' + @role + '_Name] in (select ' + @role + ' from ##roleFiltered)'

	drop table if exists ##roleUmrFiltered

	set @sql = 'select 
		[UMR_Risk_Section]
	into ##roleUmrFiltered
	from (
		select 
			umr.[UMR_Risk_Section]
			,row_number() over(partition by umr.[UMR_Risk_Section] order by umr.[UMR_Risk_Section]) as [rowN]
		from [rls_filterset_umr_' + @role + '] umr
		join [rls_filterset_rls_' + @role + '] t1 on umr.[RLS_' + @role + '_id] = t1.[id]
		join [rls_filterset_rls_' + @role + '] t2 on t1.[parent_id] = t2.[id]
		where t2.[' + @role + '] in (select [value] from ##roleValues) '
		+ @lead_condition + '
	) w
	where [rowN] = 1'
	
	exec(@sql)

	set @roleUmrJoin = 'join ##roleUmrFiltered r on r.[UMR_Risk_Section] = g.[umr_rc_sn]'

end

if @role = 'Underwriter'
begin
	set @roleValueWhereClause = ''
end

---- Parameters handling ----

drop table if exists #parametersJson
select @parameters as [parameters] into #parametersJson

drop table if exists #parameters

select
    [name]
    ,[value]
    ,row_number() over(order by [name]) as [rowN]
into #parameters
from (
    select 
        [name],
        [value]
    from #parametersJson
    cross apply openjson([parameters])
    with (
		[name] varchar(2000)
		,[value] nvarchar(max) as json
    ) as ExtractedData
) as ParsedData
where isjson([value]) = 1

drop table if exists #parametersDateRange

select
     [name]
	 ,[type]
    ,[value]
    ,row_number() over(order by [name]) as [rowN]
into #parametersDateRange
from (
    select 
        [name]
		,[type]
        ,case
            when isjson([value]) = 1 then null
            else [value] 
        end as [value]
    from #parametersJson
    cross apply openjson([parameters])
    with (
		[name] varchar(2000)
		,[type] varchar(50)
		,[value] nvarchar(max)
    ) as ExtractedData
) as ParsedData
where [value] is not null

---- Reporting Period ----

declare @reportingPeriodWhereClause varchar(2000)

if (select count(*) from #parametersDateRange) > 0
	begin
		declare @startDate varchar(10)
		declare @endDate varchar(10)
		
		drop table if exists #dateRange

		select 
			[name]
			,cast(s.[value] as date) as [date]
			,row_number() over(order by cast(s.[value] as date)) as [rowN]
		into #dateRange
		from #parametersDateRange
		cross apply string_split(trim('[""] ' from replace([value],'"','')), ',') s

		set @startDate = (select [date] from #dateRange where [rowN] = 1)
		set @endDate = (select [date] from #dateRange where [rowN] = 2)

		if (select [name] from #parametersDateRange) = 'modified_on'
			begin
				set @reportingPeriodWhereClause = ' where cast(d.[modified_on] as date) between ''' + @startDate + ''' and ''' + @endDate + ''''
			end
		else
			begin
				set @reportingPeriodWhereClause = ' where [Reporting_Period_End_Date] between ''' + @startDate + ''' and ''' + @endDate + ''''
			end
	end
else
	begin
		
		
		drop table if exists #params_reportingPeriod

		select 
			[name]
			,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [Reporting_Period_End_Date]
		into #params_reportingPeriod
		from #parameters p
		cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
		where [name] = 'Reporting_Period_End_Date'

		if (select count(*) from #params_reportingPeriod) > 0
			set @reportingPeriodWhereClause = ' where d.[Reporting_Period_End_Date] in (select [Reporting_Period_End_Date] from #params_reportingPeriod)'
		else
			set @reportingPeriodWhereClause = ''

	end

---- Temporary tables for parameter values

drop table if exists #params_underwriter

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [Underwriter]
into #params_underwriter
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'Underwriter_Name'

drop table if exists #params_coverholder

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [Coverholder]
into #params_coverholder
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'Coverholder_Name'

drop table if exists #params_tpa

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [TPA]
into #params_tpa
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'TPA_Name'

drop table if exists #params_umr

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [UMR]
into #params_umr
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'Unique_Market_Reference_UMR'

declare @umrWhereClause nvarchar(max) = ''

if (select count(*) from #params_umr) > 0
	set @umrWhereClause = ' and [umr] in (select [UMR] from #params_umr)'
	 

---- Tables with [UMR_Risk_Section] for each parameter ----

drop table if exists ##umrRSFiltered_underwriter

create table ##umrRSFiltered_underwriter (
	[UMR_Risk_Section] nvarchar(256) null
)	

declare @paramsUnderwriterWhereClause varchar(100) = ''

if (select count(*) from #params_underwriter) > 0
begin
	
	insert ##umrRSFiltered_underwriter ([UMR_Risk_Section])
	select 
		[UMR_Risk_Section]
	from (
		select 
			umr.[UMR_Risk_Section]
			,row_number() over(partition by umr.[UMR_Risk_Section] order by umr.[UMR_Risk_Section]) as [rowN]
		from [rls_filterset_umr_underwriter] umr
		join [rls_filterset_rls_underwriter] t1 on umr.[RLS_Underwriter_id] = t1.[id]
		left join [rls_filterset_rls_underwriter] t2 on t1.[parent_id] = t2.[id]
		where t2.[underwriter] in (select [underwriter] from #params_underwriter)
	) w
	where [rowN] = 1

	set @paramsUnderwriterWhereClause = 'where t2.[underwriter] in (select [underwriter] from #params_underwriter)'

end

drop table if exists ##umrRSFiltered_coverholder

create table ##umrRSFiltered_coverholder (
	[UMR_Risk_Section] nvarchar(256) null
)

declare @paramsCoverholderWhereClause varchar(100) = ''

if (select count(*) from #params_coverholder) > 0
begin
	
	insert ##umrRSFiltered_coverholder ([UMR_Risk_Section])
	select 
		[UMR_Risk_Section]
	from (
		select 
			umr.[UMR_Risk_Section]
			,row_number() over(partition by umr.[UMR_Risk_Section] order by umr.[UMR_Risk_Section]) as [rowN]
		from [rls_filterset_umr_coverholder] umr
		join [rls_filterset_rls_coverholder] t1 on umr.[RLS_Coverholder_id] = t1.[id]
		left join [rls_filterset_rls_coverholder] t2 on t1.[parent_id] = t2.[id]
		where t2.[coverholder] in (select [coverholder] from #params_coverholder)
	) w
	where [rowN] = 1

	set @paramsCoverholderWhereClause = 'where t2.[coverholder] in (select [coverholder] from #params_coverholder)'

end

drop table if exists ##umrRSFiltered_tpa

create table ##umrRSFiltered_tpa (
	[UMR_Risk_Section] nvarchar(256) null
)

declare @paramsTpaWhereClause varchar(100) = ''

if (select count(*) from #params_tpa) > 0
begin
	
	insert ##umrRSFiltered_tpa ([UMR_Risk_Section])
	select 
		[UMR_Risk_Section]
	from (
		select 
			umr.[UMR_Risk_Section]
			,row_number() over(partition by umr.[UMR_Risk_Section] order by umr.[UMR_Risk_Section]) as [rowN]
		from [rls_filterset_umr_tpa] umr
		join [rls_filterset_rls_tpa] t1 on umr.[RLS_TPA_id] = t1.[id]
		left join [rls_filterset_rls_tpa] t2 on t1.[parent_id] = t2.[id]
		where t2.[tpa] in (select [tpa] from #params_tpa)
	) w
	where [rowN] = 1

	set @paramsTpaWhereClause = 'where t2.[TPA] in (select [TPA] from #params_tpa)'

end

drop table if exists ##umrRSFiltered


---- Combing all umrRSFiltered tables

drop table if exists #tempUnion

;with cte as (
	select 
		'##umrRSFiltered_underwriter' as [tableName]
		,count(*) as [count]
	from ##umrRSFiltered_underwriter
	union all
	
	select 
		'##umrRSFiltered_coverholder' as [tableName]
		,count(*) as [count]
	from ##umrRSFiltered_coverholder
	
	union all
	
	select 
		'##umrRSFiltered_tpa' as [tableName]
		,count(*) as [count]
	from ##umrRSFiltered_tpa
)

---- Creating sql query to handle tables intersecting 

select
	[tableName]
	,[count]
	,row_number() over(order by [tableName]) as [rowN]
into #tempUnion
from cte
where [count] > 0


declare @tablesNumber tinyint = (select count(*) from #tempUnion where [count] > 0)
declare @i tinyint = 1
declare @currentTable varchar(30)
set @sql = ''

while @i <= @tablesNumber
	begin
		
		if @i <> 1
			set @sql = @sql + ' intersect '

		set @currentTable = (select [tableName] from #tempUnion where [rowN] = @i)

		if @i = 1
			set @sql = @sql + 'select [UMR_Risk_Section] into ##umrRSFiltered from ' + @currentTable
		else
			set @sql = @sql + 'select [UMR_Risk_Section] from ' + @currentTable

		set @i = @i + 1

	end

exec(@sql)


declare @umrRSFilteredJoin varchar(100) = ''

if object_id('tempdb..##umrRSFiltered') is not null
	set @umrRSFilteredJoin = 'join ##umrRSFiltered f on f.[UMR_Risk_Section] = d.[UMR_Risk_Section]'


---- UMR Critera - active/inactive handling

declare @bdxStatus char(5)

if @umr_criteria = 'Active'
	set @bdxStatus = '1'
else if @umr_criteria = 'Inactive'
	set @bdxStatus = '0'
else set @bdxStatus = '0,1'

---- Create table with filtered [umr_rc_cc_sn]

drop table if exists ##globalUmrFiltered

set @sql = 'select 
				g.[umr_rc_cc_sn]
			into ##globalUmrFiltered
			from [dbo].[rls_filterset_globalumr] g '
			+ @roleUmrJoin + '
			where g.[bdx_status] in (' +  @bdxStatus + ') '
			+ @umrWhereClause

exec(@sql)


if @reportingPeriodWhereClause <> ''
	set @roleValueWhereClause = replace(@roleValueWhereClause, 'where', 'and')

---- Get data from datarows and filter on specified conditions



--if @reportingPeriodWhereClause = '' and @roleValueWhereClause = ''
--	set @lead_condition = replace(replace(@lead_condition, 'and d.[lead]', 'where d.[lead]'), 'and exists', 'where exists')

drop table if exists ##datarowsUMR

set @sql = 'select
	[Unique_Market_Reference_UMR]
	,[UMR_Risk_Section]
	,[UMR_Risk_Cat_Section]
	,[Reporting_Period_End_Date]
into ##datarowsUMR
from (
	select 
		d.[UMR_Risk_Section]
		,d.[Unique_Market_Reference_UMR]
		,d.[UMR_Risk_Cat_Section]
		,d.[Reporting_Period_End_Date]
		,row_number() over(partition by d.[UMR_Risk_Cat_Section],d.[Reporting_Period_End_Date] order by d.[UMR_Risk_Cat_Section],d.[Reporting_Period_End_Date]) as [rowN]
	from [dbo].[Log_rig_datarows_dedupe] d
	join ##globalUmrFiltered g on g.[umr_rc_cc_sn] = d.[UMR_Risk_Cat_Section]'
	+ @umrRSFilteredJoin 
	+ @reportingPeriodWhereClause + 
	--+ @roleValueWhereClause + '
'
) w
where [rowN] = 1'


exec(@sql)


---- TPA ----

drop table if exists ##tpa

if @role = 'TPA'
	set @sql = 'select 
		[TPA]
	into ##tpa
	from ##roleFiltered'

else
	set @sql = 'select 
		[TPA]
	into ##tpa
	from (
		select 
			t2.[TPA]
			,row_number() over(partition by t2.[TPA] order by t2.[TPA]) as [rowN]
		from [rls_filterset_umr_tpa] umr
		join [rls_filterset_rls_tpa] t1 on umr.[RLS_TPA_id] = t1.[id]
		left join [rls_filterset_rls_tpa] t2 on t1.[parent_id] = t2.[id]
		join ##datarowsUMR d on d.[UMR_Risk_Section] = umr.[UMR_Risk_Section]'
		+ @paramsTpaWhereClause + '
	) w
	where [rowN] = 1'

exec(@sql)


---- Coverholder ----

drop table if exists ##coverholder

if @role = 'Coverholder'
	set @sql = 'select 
		[Coverholder]
	into ##coverholder
	from ##roleFiltered'

else	
	set @sql = 'select 
		[coverholder] as [Coverholder]
	into ##coverholder
	from (
		select 
			t2.[coverholder]
			,row_number() over(partition by t2.[coverholder] order by t2.[coverholder]) as [rowN]
		from [rls_filterset_umr_coverholder] umr
		join [rls_filterset_rls_coverholder] t1 on umr.[RLS_Coverholder_id] = t1.[id]
		left join [rls_filterset_rls_coverholder] t2 on t1.[parent_id] = t2.[id]
		join ##datarowsUMR d on d.[UMR_Risk_Section] = umr.[UMR_Risk_Section]'
		+ @paramsCoverholderWhereClause + '
	) w
	where [rowN] = 1'

exec(@sql)

---- UMR ----

drop table if exists #umr

select distinct
	[Unique_Market_Reference_UMR]
into #umr
from ##datarowsUMR


---- Reporting Period ----

drop table if exists #reportingPeriod

select distinct
	[Reporting_Period_End_Date]
into #reportingPeriod
from ##datarowsUMR
order by [Reporting_Period_End_Date] desc


---- Underwriter ----

drop table if exists ##underwriter

if @role = 'Underwriter'
	set @sql = 'select 
		[Underwriter]
	into ##underwriter
	from ##roleFiltered'

else
	set @sql = 'select 
		[underwriter] as [Underwriter]
	into ##underwriter
	from (
		select 
			t2.[underwriter]
			,row_number() over(partition by t2.[underwriter] order by t2.[underwriter]) as [rowN]
		from [rls_filterset_umr_underwriter] umr
		join [rls_filterset_rls_underwriter] t1 on umr.[RLS_Underwriter_id] = t1.[id]
		left join [rls_filterset_rls_underwriter] t2 on t1.[parent_id] = t2.[id]
		join ##datarowsUMR d on d.[UMR_Risk_Section] = umr.[UMR_Risk_Section]'
		+ @paramsUnderwriterWhereClause + '
	) w
	where [rowN] = 1'

exec(@sql)


---- Final Select

select 
	'TPA' as [Field]
	,[TPA] as [FieldName]
from ##tpa

union all

select 
	'Coverholder' as [Field]
	,[Coverholder] as [FieldName]
from ##coverholder

union all

select 
	'Unique_Market_Reference_UMR' as [Field]
	,[Unique_Market_Reference_UMR] as [FieldName]
from #umr

union all

select 
	'Reporting_Period_End_Date' as [Field]
	,convert(varchar(10),[Reporting_Period_End_Date]) as [FieldName]
from #reportingPeriod

union all

select 
	'Underwriter' as [Field]
	,[Underwriter] as [FieldName]
from ##underwriter


end
