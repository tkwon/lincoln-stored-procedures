create or alter procedure [export].[sp_getData]
	@flags varchar(255)
	,@parameters nvarchar(max)
	,@columns nvarchar(max)
	,@group_by_selection varchar(100)
	,@id varchar(50)
	,@group_by varchar(255)
	,@umr_criteria varchar(20)
	,@export_by varchar(50)

as

begin

declare @sql nvarchar(max)
declare @role varchar(20) = ''
declare @rlsWhereClause varchar(1000)

declare @export_by_column varchar(20) = case @export_by
											when 'umr-risk_code-lloyds_cat_code-section_no' then '[umr_rc_cc_sn]'
											when 'umr-risk_code-lloyds_cat_code' then '[umr_rc_cc]'
											when 'umr-lloyds_cat_code' then '[umr_cc]'
											when 'umr-risk_code' then '[umr_rc]'
											when 'section_no' then '[sn]'
											when 'lloyds_cat_code' then '[cc]'
											when 'umr' then '[umr]'
										end

---- No grouping case -----
set @group_by_selection = case 
							when @group_by_selection = '' then 'None'
							else @group_by_selection end

----- Get parameters and save each parameters value in separate temp table

drop table if exists #parametersJson
drop table if exists #parametersDateRange
drop table if exists #parameters


select @parameters as [parametrs] into #parametersJson


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
    cross apply openjson([parametrs])
    with (
		[name] varchar(2000)
		,[type] varchar(50)
		,[value] nvarchar(max)
    ) as ExtractedData
) as ParsedData
where [value] is not null

select
    [name]
    , [value]
     ,row_number() over(order by [name]) as [rowN]
into #parameters
from (
    select 
        [name],
        [value]
    from #parametersJson
    cross apply openjson([parametrs])
    with (
		[name] varchar(2000)
		,[value] nvarchar(max) as json
    ) as ExtractedData
) as ParsedData
where isjson([value]) = 1

---- Reporting Period -----

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
				set @reportingPeriodWhereClause = ' and cast(d.[modified_on] as date) between ''' + @startDate + ''' and ''' + @endDate + ''''
			end
		else
			begin
				set @reportingPeriodWhereClause = ' and [Reporting_Period_End_Date] between ''' + @startDate + ''' and ''' + @endDate + ''''
			end

	end
else
	begin
		drop table if exists #reportingPeriodEndDate
		
		select 
			[name]
			,cast(ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as date) as [date]
		into #reportingPeriodEndDate
		from #parameters p
		cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
		where [name] = 'Reporting_Period_End_Date'
		
		
		if (select count(*) from #reportingPeriodEndDate) > 0
			set @reportingPeriodWhereClause = ' and [Reporting_Period_End_Date] in (select [date] from #reportingPeriodEndDate)'
		else
			set @reportingPeriodWhereClause = ''

	end


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
declare @currentName varchar(25) = ''
--declare @currentValue char(1)

while @i <= @rows
	begin
		set @currentName = (select [name] from #flags where [rowN] = @i and [value] = 1)
		if @currentName is null
			begin
				set @currentName = 'Missing'
			end
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

drop table if exists #columnsOrder

create table #columnsOrder (
	[column] varchar(50) null
	,[rowN] smallint identity(1,1)
)

insert into #columnsOrder ([column]) 
values ('Coverholder_Name'), ('TPA_Name'), ('Agreement'), ('Unique_Market_Reference_UMR'), ('Binder_Contract_Inception'), ('Binder_Contract_Expiry'), ('Reporting_Period_End_Date'), ('Class_of_Business'), ('Risk_Code'), ('Section_No'), ('Original_Currency'), ('Settlement_Currency'), ('Rate_of_Exchange'), ('Certificate_Reference'), ('Claim_Reference'), ('Insured_Full_Name'), ('Insured_State'), ('Insured_Country'), ('Location_of_Risk_State'), ('Location_of_Risk_County'), ('Risk_Inception_Date'), ('Risk_Expiry_Date'), ('Period_of_Cover'), ('Loss_State'), ('Location_of_Loss_Country'), ('Cause_of_Loss'), ('Loss_Description'), ('Date_of_Loss_From'), ('Date_of_Loss_To'), ('Date_Claim_Made'), ('Claim_Status'), ('Refer_to_Underwriters'), ('Denial'), ('Litigation_status'), ('Claimant_Name'), ('Loss_County'), ('State_of_Filing'), ('PCS_Code'), ('Medicare_United_States_Bodily_Injury'), ('Medicare_Eligibility_Check_Performed'), ('Medicare_Outcome_of_Eligibility_Status_Check'), ('Medicare_Conditional_Payments'), ('Medicare_MSP_Compliance_Services'), ('Paid_This_Month_Indemnity'), ('Paid_This_Month_Fees'), ('Previously_Paid_Indemnity'), ('Previously_Paid_Fees'), ('Reserve_Indemnity'), ('Reserve_Fees'), ('Change_This_Month_Indemnity'), ('Change_This_Month_Fees'), ('Total_Incurred_Indemnity'), ('Total_Incurred_Fees'), ('Coverholder_PIN'), ('Reporting_Period_Start_Date'), ('Type_of_Insurance'), ('Policy_or_Group_Ref'), ('Insured_Address'), ('Insured_Postcode_Zip_Code_or_similar'), ('Location_of_Risk_Location_ID'), ('Location_of_Risk_Address'), ('Location_of_Risk_Postcode_Zip_Code_or_similar'), ('Deductible_Amount'), ('Deductible_Basis'), ('Sums_Insured_Amount'), ('Location_of_Loss_Address'), ('Location_of_Loss_Postcode_Zip_Code_or_similar'), ('Date_Closed'), ('Lloyds_Cat_Code'), ('Catastrophe_Name'), ('Paid_this_month_Expenses'), ('Paid_this_month_Attorney_Coverage_Fees'), ('Paid_this_month_Adjusters_Fees'), ('Paid_this_month_Defence_Fees'), ('Paid_this_month_TPA_Fees'), ('Paid_this_month_Bank_Charges'), ('Previously_Paid_Expenses'), ('Previously_Paid_Attorney_Coverage_Fees'), ('Previously_Paid_Adjusters_Fees'), ('Previously_Paid_Defence_Fees'), ('Previously_Paid_TPA_Fees'), ('Previously_Paid_Bank_Charges'), ('Reserve_Expenses'), ('Reserve_Attorney_Coverage_Fees'), ('Reserve_Adjusters_Fees'), ('Reserve_Defence_Fees'), ('Reserve_TPA_Fees'), ('Reclosed_Date'), ('Net_Recovery'), ('Total_Incurred'), ('Reg_No_of_Vehicle_etc'), ('Ceded_Reinsurance'), ('Plan'), ('Patient_Name'), ('Treatment_Type'), ('Country_of_Treatment'), ('Date_of_Treatment'), ('Expert_Role'), ('Expert_Firm'), ('Expert_Reference_No'), ('Expert_Address'), ('Expert_State'), ('Expert_Postcode_Zip_Code_or_similar'), ('Expert_Country'), ('Notes'), ('Date_Claim_Opened'), ('Ex_gratia_payment'), ('Claim_First_Notification_Acknowledgement_Date'), ('Date_First_Reserve_Established'), ('Date_Coverage_Confirmed'), ('Diary_date'), ('Peer_review_date'), ('Date_Claim_Amount_Agreed'), ('Date_Claims_Paid'), ('Date_of_Subrogation'), ('Date_Reopened'), ('Date_Claim_Denied'), ('Reason_for_Denial'), ('Date_claim_withdrawn'), ('Amount_Claimed'), ('Severity_of_loss'), ('Other_Disbursements'), ('Initial_Reserve_Value'), ('Date_of_Last_Correspondence_Sent'), ('Date_of_Last_Correspondence_Received'), ('Claims_Examiner'), ('Premises_Number'), ('Building_Number'), ('Class_Code'), ('Subrogation_Recovered_This_Month'), ('Subrogation_Previously_Recovered'), ('Total_Subrogation_Recovered'), ('Salvage_Recovered_This_Month'), ('Salvage_Previously_Recovered'), ('Total_Salvage_Recovered'), ('Deductible_Recovered_This_Month'), ('Deductible_Previously_Recovered'), ('Total_Deductible_Recovered'), ('Gross_Recovery_Received_This_Month'), ('Gross_Recovery_Previously_Received'), ('Gross_Recovery_Total'), ('Recovery_Fees_Paid_This_Month'), ('Recovery_Fees_Paid_Previously_Paid'), ('Recovery_Fees_Total'), ('Driver_Age'), ('Cargo_Hauled'), ('Reefer_Claim'), ('Reefer_Age'), ('Cargo_Total_Insured_Value'), ('Vehicle_Unit_Age'), ('Vehicles_Total_Insured_Value'), ('Towing_Storage_Fees'), ('Trailer_Interchange'), ('Non_Owned_Trailer'), ('MTC_Peril'), ('Fault'), ('Distance'), ('Year_of_Account');


drop table if exists #columns

select 
	value as [Column]
	,'d.[' + value + ']' as [ColumnFinal]
	,row_number() over(partition by value order by value) as [rowN]
into #columns
from openjson(@columns)


-- delete duplicated columns
delete from #columns where [rowN] <> 1

-- delete invalid columns
delete from #columns where [Column] in (
 'Date_of_Loss_To'
,'Reg_No_of_Vehicle_etc'
,'Ceded_Reinsurance'
,'Plan'
,'Patient_Name'
,'Treatment_Type'
,'Country_of_Treatment'
,'Date_of_Treatment'
,'BDX_Key'
)


drop table if exists #columnsFinal

select 
	c.[Column]
	,c.[ColumnFinal]
	,co.[rowN]
into #columnsFinal
from #columns c
join #columnsOrder co on co.[column] = c.[Column]
order by co.[rowN]

create clustered index [columnsIX] on #columnsFinal([rowN])

declare @columnsSelect nvarchar(max)

set @columnsSelect = (select string_agg([ColumnFinal],', ') from #columnsFinal)

drop table if exists #columnsIndex

select [Column]
into #columnsIndex
from #columnsFinal
where [Column] <> 'Reporting_Period_End_Date'

declare @columnsIndex nvarchar(max)

set @columnsIndex = (select string_agg([Column],', ') from #columnsIndex)


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

declare @activeWhereClause varchar(50)

if @umr_criteria = 'Active'
	set @activeWhereClause = 'where g.[bdx_status] = 1'

if @umr_criteria = 'Inactive'
	set @activeWhereClause = 'where g.[bdx_status] = 0'

if @umr_criteria = 'All'
	set @activeWhereClause = 'where g.[bdx_status] in (0,1)'

drop table if exists ##activeUmr

set @sql = 'select distinct
	g.[umr_rc_cc_sn]
	into ##activeUmr
	from [dbo].[rls_filterset_globalumr] g
	join ##umr u on u.[umr_rc_sn] = g.[umr_rc_sn]'
	+ @activeWhereClause

exec(@sql)


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
	[UMR]
	,left([UMR],5) as [Broker]
into #brokers
from ##umr

---- Assigning flags to UMRs -----

drop table if exists ##temp
set @sql = 'select ' + @group_by_columns + ' 
				,[Critical_Error_Flag]
				,[Non_Critical_Flag]
				,row_number() over(partition by ' + @group_by_columns + ' order by ' + @group_by_columns + ', [Critical_Error_Flag] desc, [Non_Critical_Flag] desc) as [rowN]
			into ##temp
			from (	
				select distinct ' + @group_by_columns + '
						,dir.[Critical_Error_Flag]
						,dir.[Non_Critical_Flag]
				from [dbo].[rig_datarows] d
				join ##activeUmr a on a.[umr_rc_cc_sn] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')
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

drop table if exists ##tempExport

set @sql = 'select 
				u.[Underwriter], b.[Broker], ''None'' as [None]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'') as [umr_rc_cc_sn]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''),''0'')  + ''_'' + coalesce(nullif(d.[Section_No],''''),''0'') as [umr_rc_sn]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''),''0'')  + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''),''0'') as [umr_rc_cc]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''),''0'') as [umr_cc]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''),''0'') as [umr_rc]
				,coalesce(nullif(d.[Section_No],''''), ''0'') as [sn]
				,coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') as [cc]
				,d.[Unique_Market_Reference_UMR] as [umr],' 
			+ @columnsSelect + '
			into ##tempExport
			from [dbo].[rig_datarows] d
			join ##activeUmr a on a.[umr_rc_cc_sn] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')'
			+ @umr_flags_join + '
			left join #underwriters u on u.[UMR_Risk_Section] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')
			left join #brokers b on b.[UMR] = d.[Unique_Market_Reference_UMR]'
			+ @flagsWhereClause
			+ @reportingPeriodWhereClause 
			+ @tpaWhereClause
			--+ @coverholderWhereClause
			+ @underwriterWhereClause

exec(@sql)


drop table if exists ##coverholderGroup

set @sql = 'select
	[umr_rc_sn]
	,[Coverholder_final]
into ##coverholderGroup
from (
	select 
		e.[umr_rc_sn]
		,c.[RLS_Coverholder_id]
		,crls.[coverholder]
		,crls.[parent_id]
		,case 
			when crls.[parent_id] is null then coalesce(crls.[coverholder],''Missing'')
			else coalesce(ccrls.[coverholder],''Missing'')
		end as [Coverholder_final]
		,row_number() over(partition by e.[umr_rc_sn] order by crls.[parent_id]) as [rowN]
	from ##tempExport e
	join [dbo].[rls_filterset_umr_coverholder] c on c.[UMR_Risk_Section] = e.[umr_rc_sn]
	join [dbo].[rls_filterset_rls_coverholder] crls on crls.[id] = c.[RLS_Coverholder_id]
	left join [dbo].[rls_filterset_rls_coverholder] ccrls on ccrls.[id] = crls.[parent_id]
) w
where [rowN] = 1
'

exec(@sql)

set @sql = 'drop table if exists ##exportGetData'
exec(@sql)

set @sql = 'select 
	c.[Coverholder_final] as [Coverholder]
	,e.*
	into ##exportGetData
	from ##tempExport e
	left join ##coverholderGroup c on c.[umr_rc_sn] = e.[umr_rc_sn]'

exec(@sql)

---- create index on new table ----

--declare @createindexsql nvarchar(max)

--set @createindexsql = 'create nonclustered index ix_nc_exportGetData_' + @id + ' on [export].[getData_' + @id + '] ([Reporting_Period_End_Date], [umr_rc_cc_sn], [' + @group_by_selection + ']) include (' + @columnsIndex + ')'

--exec(@createindexsql)

---- 6) Grouping data ----

drop table if exists ##groupedExport

--declare @group_by_selection_folder varchar(25) =  case when @group_by_selection = 'Coverholder_Name' then 'Coverholder' else @group_by_selection end
--declare @coverholder_prefix varchar(5) = case when @group_by_selection = 'Coverholder_Name' then 'cg.' else '' end
--set @group_by_selection = case when @group_by_selection = 'Coverholder_Name' then 'Coverholder_final' else @group_by_selection end

set @sql = 'select
	[Reporting_Period_End_Date]
	,' + @export_by_column + '
	,[Coverholder]
	,[Reporting_Period_End_Date_Folder]
	,coalesce(nullif([Group_By_Selection_Folder],''None''),'''') as [Group_By_Selection_Folder]
	,[FileName]
	,[RowsNumber]
into ##groupedExport
from (
	select distinct
		''RptDate-'' + convert(varchar(10), [Reporting_Period_End_Date], 23) as [Reporting_Period_End_Date_Folder]
		,coalesce([' + @group_by_selection + '],'''') as [Group_By_Selection_Folder]
		,[Coverholder] + ''-'' + replace(e.'+ @export_by_column + ',''_'',''-'') + ''-'' + ''(RptDate-'' + convert(varchar(10), [Reporting_Period_End_Date], 23) + '')'' as [FileName]
		,count(*) as [RowsNumber]
		,e.' + @export_by_column + '
		,[Reporting_Period_End_Date]
		,[Coverholder]
	from ##exportGetData e
	group by [Reporting_Period_End_Date],[' + @group_by_selection + '] ,e.' + @export_by_column + ',[Coverholder]
) w
order by [Reporting_Period_End_Date_Folder],[Group_By_Selection_Folder] ,[FileName]'

exec(@sql)


set @sql = 'drop table if exists [export].[getData_' + @id + ']'
exec(@sql)

set @sql = 'select
	 [rowN] as [FileNumber]
	,[Reporting_Period_End_Date_Folder]
	,[Group_By_Selection_Folder]
	,[FileName]
	,' + @columnsSelect + '
	,[BDX_Key]
into [export].[getData_' + @id + ']
from (
	select
		 g.[Reporting_Period_End_Date_Folder] 
		,g.[Group_By_Selection_Folder]
		,g.[FileName]
		,' + @columnsSelect + '
		,dense_rank() over(order by g.[FileName]) as [rowN]
		,d.[umr_rc_cc_sn] as [BDX_Key]
	from ##exportGetData d
	join ##groupedExport g on g.[Reporting_Period_End_Date] = d.[Reporting_Period_End_Date]
							and g.' + @export_by_column + ' = d.' + @export_by_column + '
							and g.[Coverholder] = d.[Coverholder]
							and coalesce(nullif(g.[Group_By_Selection_Folder],''''),''None'') = d.[' + @group_by_selection + ']
) d
order by [rowN]'



exec(@sql)


end


