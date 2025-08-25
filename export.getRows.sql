create or alter procedure [export].[sp_getData]
---- list of parameters
	@flags varchar(255)
	,@parameters nvarchar(max)
	,@columns nvarchar(max)
	,@group_by_selection varchar(100)
	,@id varchar(50)
	,@group_by varchar(255)
	,@umr_criteria varchar(20)
	,@export_by varchar(100)
	,@role varchar(20)
	,@role_value varchar(255)
	,@uw_option varchar(10) = 'Lead'

as

begin

---- Declaring variables

declare @sql nvarchar(max)
declare @rlsFilterset varchar(20) = ''
declare @rlsWhereClause varchar(1000)

--declare @isLead bit = case
--						when @uw_option = 'Lead' then 1

declare @export_by_column varchar(50) = case 
											when @export_by = 'umr-risk_code-lloyds_cat_code-section_no-settlement_currency-year_of_account' then '[umr_rc_cc_sn_currency_year]'
											when @export_by = 'umr-risk_code-lloyds_cat_code-section_no' then '[umr_rc_cc_sn]'
											when @export_by = 'umr-risk_code-lloyds_cat_code' then '[umr_rc_cc]'
											when @export_by = 'umr-lloyds_cat_code' then '[umr_cc]'
											when @export_by = 'umr-risk_code' then '[umr_rc]'
											when @export_by = 'section_no' then '[sn]'
											when @export_by = 'lloyds_cat_code' then '[cc]'
											when @export_by = 'umr' then '[umr]'
											when @export_by = 'tpa' then '[TPA]'
											when @export_by = 'coverholder' then '[Coverholder]'
											when @export_by = 'underwriter' then '[Underwriter]'
											when @export_by = '' and @group_by_selection = '' then '[Combined]'
											when @export_by = '' and @group_by_selection = 'Coverholder' then '[Coverholder]'
											when @export_by = '' and @group_by_selection = 'Underwriter' then '[Underwriter]'
											when @export_by = '' and @group_by_selection = 'Broker' then '[Broker]'
										end

declare @transactions_join varchar(400) = case @export_by
		when 'umr-risk_code-lloyds_cat_code-section_no' then 't.[umr] + ''_'' + coalesce(nullif(t.[risk_code],''''), ''0'') + ''_'' + coalesce(nullif(t.[cat_code],''''), ''0'') + ''_'' + coalesce(nullif(t.[section_No],''''), ''0'') = e.[umr_rc_cc_sn]'
		when 'umr-risk_code-lloyds_cat_code-section_no-settlement_currency-year_of_account' then 't.[bdx_key] = e.[umr_rc_cc_sn_currency_year]'
		when 'umr-risk_code-lloyds_cat_code' then 't.[umr] + ''_'' + coalesce(nullif(t.[risk_code],''''), ''0'') + ''_'' + coalesce(nullif(t.[cat_code],''''), ''0'') = e.[umr_rc_cc]'
		when 'umr-risk_code' then 't.[umr] + ''_'' + coalesce(nullif(t.[risk_code],''''), ''0'') = e.[umr_rc]'
		when 'umr-lloyds_cat_code' then 't.[umr] + ''_'' + coalesce(nullif(t.[cat_code],''''), ''0'') = e.[umr_cc]'
		when 'section_no' then 'coalesce(nullif(t.[section_no],''''), ''0'') = e.[sn]'
		when 'lloyds_cat_code' then 'coalesce(nullif(t.[cat_code],''''), ''0'') = e.[cc]'
		when 'umr' then 't.[umr] = e.[umr]'
		when 'tpa' then 't.[tpa] = e.[TPA]'
		when 'coverholder' then 't.[coverholder] = e.[Coverholder]'
		when 'underwriter' then 't.[underwriter] = e.[Underwriter]'
		else 't.[bdx_key] = e.[umr_rc_cc_sn_currency_year]'
end


declare @ifCoverholderFileName varchar(20) = case 
												when @export_by in ('lloyds_cat_code','section_no','tpa', 'coverholder','underwriter','') then ''
												else '[Coverholder] + ''-'''
											 end

declare @ifCoverholderColumn varchar(20) = case
												when @export_by = 'coverholder' or (@export_by = '' and @group_by_selection = 'Coverholder') then ''
												else ' ,[Coverholder]'
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

drop table if exists ##tpa

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [TPA]
into ##tpa
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'TPA_Name'

if (select count(*) from ##tpa) = 0 and @role = 'TPA'
begin
	set @sql = 'drop table if exists ##tpa'
	exec(@sql)

	set @sql = '
		select ''' + 
			@role + ''' as [name]
			,''' + @role_value + ''' as [TPA]
		into ##tpa
	'

	exec(@sql)
end

if (select count(*) from ##tpa) > 0
	set @rlsFilterset = 'TPA'
if (select count(*) from ##tpa) > 0
	set @rlsWhereClause = 'select [TPA] from ##tpa'
	
---- Underwriter -----

drop table if exists ##underwriter

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [Underwriter]
into ##underwriter
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'Underwriter_Name'

if (select count(*) from ##underwriter) = 0 and @role = 'Underwriter'
begin
	set @sql = 'drop table if exists ##underwriter'
	exec(@sql)

	set @sql = '
		select ''' + 
			@role + ''' as [name]
			,''' + @role_value + ''' as [Underwriter]
		into ##underwriter
	'

	exec(@sql)
end


if (select count(*) from ##underwriter) > 0
	set @rlsFilterset = 'Underwriter'
if (select count(*) from ##underwriter) > 0
	set @rlsWhereClause = 'select [Underwriter] from ##underwriter'


---- Coverholder ----

drop table if exists ##coverholder

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [Coverholder]
into ##coverholder
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'Coverholder_Name'

if (select count(*) from ##coverholder) = 0 and @role = 'Coverholder'
begin
	set @sql = 'drop table if exists ##coverholder'
	exec(@sql)

	set @sql = '
		select ''' + 
			@role + ''' as [name]
			,''' + @role_value + ''' as [Coverholder]
		into ##coverholder
	'

	exec(@sql)
end

if (select count(*) from ##coverholder) > 0
	set @rlsFilterset = 'Coverholder'
if (select count(*) from ##coverholder) > 0
	set @rlsWhereClause = 'select [Coverholder] from ##coverholder'


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

if (select count(*) from #umr) > 0
	set @transactions_join = @transactions_join + ' and t.[umr] = e.[umr]'
else
	set @transactions_join = @transactions_join


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

while @i <= @rows
	begin
		set @currentName = (select [name] from #flags where [rowN] = @i and [value] = 1)
		if @currentName is null
			begin
				set @currentName = 'Missing'
			end

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

if (select [value] from #flags where [name] = 'Non_Critical_Flag') = 0 and (select [value] from #flags where [name] = 'Critical_Error_Flag') = 0
	set @flagsWhereClause = ' where uf.[Flag] = ''No Flag'''

if (select [value] from #flags where [name] = 'Non_Critical_Flag') = 1 and (select [value] from #flags where [name] = 'Critical_Error_Flag') = 0
	set @flagsWhereClause = ' where uf.[Flag] in (''No Flag'', ''Non_Critical_Flag'')'

if (select [value] from #flags where [name] = 'Non_Critical_Flag') = 0 and (select [value] from #flags where [name] = 'Critical_Error_Flag') = 1
	set @flagsWhereClause = ' where uf.[Flag] in (''No Flag'', ''Critical_Error_Flag'')'

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


-- create table with columns in final order

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

-- create table with columns used in the index

drop table if exists #columnsIndex

select [Column]
into #columnsIndex
from #columnsFinal
where [Column] <> 'Reporting_Period_End_Date'

declare @columnsIndex nvarchar(max)

set @columnsIndex = (select string_agg([Column],', ') from #columnsIndex)

-- table with columns that can be summed up (not used at the moment)

drop table if exists #columnsSum

select
	[COLUMN_NAME] as [Column]
	,'sum([' + [COLUMN_NAME] + ']) as [' + [COLUMN_NAME] + '_current_month_sum]' as [ColumnSum]
into #columnsSum
from (
	select 
	    i.[COLUMN_NAME],
	    i.[DATA_TYPE]
	from 
	    INFORMATION_SCHEMA.COLUMNS i
	join #columnsFinal c on c.[Column] = i.[COLUMN_NAME] 
	where 
	    [TABLE_NAME] = 'Log_rig_datarows_dedupe'
	    and [TABLE_SCHEMA] = 'dbo'
) w
where [DATA_TYPE] = 'numeric'

declare @columnsSum nvarchar(max) 

set @columnsSum = (select string_agg(cast([ColumnSum] as nvarchar(max)), ', ') from #columnsSum)


---- 1) find the matching Group to the value -----




if @rlsFilterset <> ''
begin

	drop table if exists ##rlsIds
	
	set @sql = 'select * into ##rlsIds
				from [dbo].[rls_filterset_rls_' + @rlsFilterset + ']
				where [parent_id] in (
					select [id] from [dbo].[rls_filterset_rls_' + @rlsFilterset + '] where ' + @rlsFilterset + ' in (' + @rlsWhereClause + ') and [parent_id] is null
		)
	'
	exec(@sql)

end
	
declare @tpaWhereClause varchar(255)

if @rlsFilterset = 'TPA'
	set @tpaWhereClause = ' and [TPA_Name] in (select [TPA] from ##rlsIds)'
else set @tpaWhereClause = ''

declare @coverholderWhereClause varchar(255)

if @rlsFilterset = 'Coverholder'
	set @coverholderWhereClause = ' and [Coverholder_Name] in (select [Coverholder] from ##rlsIds)'
else set @coverholderWhereClause = ''

declare @underwriterWhereClause varchar(255)

if @rlsFilterset = 'Underwriter'
	set @underwriterWhereClause = ' and u.[Underwriter] in (select [underwriter] from ##rlsIds)'
else set @underwriterWhereClause = ''


---- 2) Get UMRs for specified group ----
drop table if exists ##umr

create table ##umr (
	[UMR] nvarchar(128) null
	,[Risk_Code] nvarchar(64) null
	,[Section_No] nvarchar(64) null
	,[umr_rc_sn] nvarchar(256) null
)



declare @lead_condition varchar(500) = case
							when @rlsFilterset = 'Underwriter' and @uw_option = 'Lead' then ' and d.[lead] = 1'
							when @rlsFilterset = 'Underwriter' and @uw_option = 'Follow' then ' and d.[lead] = 0'
							when @rlsFilterset = 'TPA' then ' and exists (
												select 1 
												from [dbo].[rls_filterset_umr_underwriter] u 
												where u.[UMR] = d.[UMR] 
												and coalesce(nullif(u.[Risk_Code],''''), ''0'') = coalesce(nullif(d.[Risk_Code],''''), ''0'')
												and coalesce(nullif(u.[Section_No],''''), ''0'') = coalesce(nullif(d.[Section_No],''''), ''0'')
												and u.[lead] = 1)'
							else ''
						end


if @rlsFilterset <> ''
begin

	set @sql = 'insert ##umr ([UMR],[Risk_Code],[Section_No],[umr_rc_sn])
				select distinct d.[UMR], coalesce(nullif(d.[Risk_Code],''''), ''0'') as [Risk_Code], coalesce(nullif(d.[Section_No],''''), ''0'') as [Section_No]
					,d.[UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'') as [umr_rc_sn]
				from [dbo].[rls_filterset_umr_' + @rlsFilterset + '] d
				join ##rlsIds r on r.[id] = d.[RLS_'+ @rlsFilterset + '_id] '
				+ @umrWhereClause + @lead_condition
	
	exec(@sql)

end

if @rlsFilterset = ''
begin

	set @sql = 'insert ##umr ([umr_rc_sn],[UMR])
				select distinct d.[umr_rc_sn], d.[UMR]
				from [dbo].[rls_filterset_globalumr] d'
				+ @umrWhereClause
	
	exec(@sql)

end

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
				from [dbo].[Log_rig_datarows_dedupe] d
				join ##activeUmr a on a.[umr_rc_cc_sn] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')
				join [dbo].[rig_dataintegrityrules] dir on dir.[data_row_id] = d.[id] '
				+ @tpaWhereClause 
				+ @coverholderWhereClause
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


---- 5) Get rows from datarows table (based on user's columns and flags selection) -----

drop table if exists ##tempExport

set @sql = 'select 
				u.[Underwriter]
				,b.[Broker]
				,''None'' as [None]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')  + ''_'' + coalesce(nullif(d.[Settlement_Currency],''''), ''0'')  + ''_'' + coalesce(nullif(convert(char(4),d.[Year_of_Account]),''''), ''0'') as [umr_rc_cc_sn_currency_year]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'') as [umr_rc_cc_sn]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''),''0'')  + ''_'' + coalesce(nullif(d.[Section_No],''''),''0'') as [umr_rc_sn]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''),''0'')  + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''),''0'') as [umr_rc_cc]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''),''0'') as [umr_cc]
				,d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''),''0'') as [umr_rc]
				,coalesce(nullif(d.[Section_No],''''), ''0'') as [sn]
				,coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') as [cc]
				,d.[Unique_Market_Reference_UMR] as [umr]
				,'''' as [Combined],' 
			+ @columnsSelect + '
			into ##tempExport
			from [dbo].[Log_rig_datarows_dedupe] d
			join ##activeUmr a on a.[umr_rc_cc_sn] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Lloyds_Cat_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')   + ''_'' + coalesce(nullif(d.[Settlement_Currency],''''), ''0'')  + ''_'' + coalesce(nullif(convert(char(4),d.[Year_of_Account]),''''), ''0'')'
			+ @umr_flags_join + '
			left join #underwriters u on u.[UMR_Risk_Section] = d.[Unique_Market_Reference_UMR] + ''_'' + coalesce(nullif(d.[Risk_Code],''''), ''0'') + ''_'' + coalesce(nullif(d.[Section_No],''''), ''0'')
			left join #brokers b on b.[UMR] = d.[Unique_Market_Reference_UMR]'
			+ @flagsWhereClause
			+ @reportingPeriodWhereClause 
			+ @tpaWhereClause
			+ @underwriterWhereClause

exec(@sql)


-- Get final coverholder name

drop table if exists ##coverholderGroup

select
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
			when crls.[parent_id] is null then coalesce(crls.[coverholder],'Missing')
			else coalesce(ccrls.[coverholder],'Missing')
		end as [Coverholder_final]
		,row_number() over(partition by e.[umr_rc_sn] order by crls.[parent_id]) as [rowN]
	from ##tempExport e
	join [dbo].[rls_filterset_umr_coverholder] c on c.[UMR_Risk_Section] = e.[umr_rc_sn]
	join [dbo].[rls_filterset_rls_coverholder] crls on crls.[id] = c.[RLS_Coverholder_id]
	left join [dbo].[rls_filterset_rls_coverholder] ccrls on ccrls.[id] = crls.[parent_id]
) w
where [rowN] = 1


-- Get final TPA name

drop table if exists ##tpaGroup

select
	[umr_rc_sn]
	,[TPA_final]
into ##tpaGroup
from (
	select 
		e.[umr_rc_sn]
		,c.[RLS_TPA_id]
		,crls.[TPA]
		,crls.[parent_id]
		,case 
			when crls.[parent_id] is null then coalesce(crls.[TPA],'Missing')
			else coalesce(ccrls.[TPA],'Missing')
		end as [TPA_final]
		,row_number() over(partition by e.[umr_rc_sn] order by crls.[parent_id]) as [rowN]
	from ##tempExport e
	join [dbo].[rls_filterset_umr_tpa] c on c.[UMR_Risk_Section] = e.[umr_rc_sn]
	join [dbo].[rls_filterset_rls_tpa] crls on crls.[id] = c.[RLS_TPA_id]
	left join [dbo].[rls_filterset_rls_tpa] ccrls on ccrls.[id] = crls.[parent_id]
) w
where [rowN] = 1


-- create temporary table (joined ##tempExport with coverholder and TPA name)

drop table if exists ##exportGetData

select 
	c.[Coverholder_final] as [Coverholder]
	,t.[TPA_final] as [TPA]
	,e.*
	into ##exportGetData
	from ##tempExport e
	left join ##coverholderGroup c on c.[umr_rc_sn] = e.[umr_rc_sn]
	left join ##tpaGroup t on t.[umr_rc_sn] = e.[umr_rc_sn]


-----  Transactions ------

declare @currentMonth date = (select coalesce(max([Reporting_Period_End_Date]),'2099-01-01') from ##exportGetData)
declare @previousMonth date = (select eomonth(dateadd(month, -1, coalesce(max([Reporting_Period_End_Date]),'2099-01-01'))) from ##exportGetData)

drop table if exists ##currentMonthSumTemp

set @sql = 'select 
	e.' + @export_by_column + ' as [Export_Key]
	,t.[reporting_period_end_date]
	,sum(t.[amount]) as [Amount]
into ##currentMonthSumTemp
from ##exportGetData e
join [dbo].[billing_transaction] t on ' + @transactions_join + ' and t.[reporting_period_end_date] = ''' + convert(varchar(15),@currentMonth) + '''
group by e.' + @export_by_column + ',t.[reporting_period_end_date]'

exec(@sql)

drop table if exists ##currentMonthSum

select 
	'CurrentMonth' as [JoinKey]
	,sum([Amount]) as [Amount]
into ##currentMonthSum
from ##currentMonthSumTemp

drop table if exists ##previousMonthSumTemp

set @sql = 'select 
	e.' + @export_by_column + ' as [Export_Key]
	,t.[reporting_period_end_date]
	,sum(t.[amount]) as [Amount]
into ##previousMonthSumTemp
from ##exportGetData e
join [dbo].[billing_transaction] t on ' + @transactions_join + ' and t.[reporting_period_end_date] = ''' + convert(varchar(15),@previousMonth) + '''
group by e.' + @export_by_column + ',t.[reporting_period_end_date]'

exec(@sql)

drop table if exists ##previousMonthSum

select 
	'PreviousMonth' as [JoinKey]
	,sum([Amount]) as [Amount]
into ##previousMonthSum
from ##previousMonthSumTemp


---- 6) Grouping data ----

drop table if exists ##groupedExport

declare @ifExportCombined1 varchar(10) = case when @export_by_column = '[Combined]' then 'RptDate-' else '-(RptDate-' end
declare @ifExportCombined2 varchar(5) = case when @export_by_column = '[Combined]' then '' else ')' end

set @sql = 'select
	[Reporting_Period_End_Date]
	,' + @export_by_column 
	 + @ifCoverholderColumn + ' 
	,[Reporting_Period_End_Date_Folder]
	,coalesce(nullif([Group_By_Selection_Folder],''None''),'''') as [Group_By_Selection_Folder]
	,[FileName]
	,[RowsNumber]
into ##groupedExport
from (
	select distinct
		''RptDate-'' + convert(varchar(10), [Reporting_Period_End_Date], 23) as [Reporting_Period_End_Date_Folder]
		,coalesce([' + @group_by_selection + '],'''') as [Group_By_Selection_Folder]
		,'+ @ifCoverholderFileName + '+ replace(coalesce(e.'+ @export_by_column + ',''0''),''_'',''-'') +''' + @ifExportCombined1 + ''' + convert(varchar(10), [Reporting_Period_End_Date], 23) + ''' + @ifExportCombined2 + '''as [FileName]
		,count(*) as [RowsNumber]
		,e.' + @export_by_column + '
		,[Reporting_Period_End_Date]
		' + @ifCoverholderColumn + '
	from ##exportGetData e
	group by [Reporting_Period_End_Date],[' + @group_by_selection + '] ,e.' + @export_by_column + ',[Coverholder]
) w
order by [Reporting_Period_End_Date_Folder],[Group_By_Selection_Folder] ,[FileName]'

exec(@sql)


---- insert grouped data into final table in export schema

set @sql = 'drop table if exists [export].[getData_' + @id + ']'
exec(@sql)

set @sql = 'select
	 [rowN] as [FileNumber]
	,[Reporting_Period_End_Date_Folder]
	,[Group_By_Selection_Folder]
	,[FileName]
	,' + @columnsSelect + '
	,[BDX_Key]
	,[current_month_sum]
	,[previous_month_sum]
into [export].[getData_' + @id + ']
from (
	select
		 g.[Reporting_Period_End_Date_Folder] 
		,g.[Group_By_Selection_Folder]
		,g.[FileName]
		,' + @columnsSelect + '
		,dense_rank() over(order by g.[FileName]) as [rowN]
		,d.[umr_rc_cc_sn_currency_year] as [BDX_Key]
		,cm.[Amount] as [current_month_sum]
		,pm.[Amount] as [previous_month_sum]
	from ##exportGetData d
	join ##groupedExport g on g.[Reporting_Period_End_Date] = d.[Reporting_Period_End_Date]
							and coalesce(g.' + @export_by_column + ',''None'') = coalesce(d.' + @export_by_column + ',''None'')
							and g.[Coverholder] = d.[Coverholder]
							and coalesce(nullif(g.[Group_By_Selection_Folder],''''),''None'') = coalesce(d.[' + @group_by_selection + '],''None'')
	left join ##currentMonthSum cm on cm.[JoinKey] = ''CurrentMonth''
	left join ##previousMonthSum pm on pm.[JoinKey] = ''PreviousMonth''
) d
order by [rowN]'

exec(@sql)

end


