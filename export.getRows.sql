
create procedure [export].[sp_getData]

as

begin

-- declare variables which will be passed later as procedure parameters

declare @flags varchar(255) = '[
      {
         "name":"Non_Critical_Flag",
         "value":0
      },
      {
         "name":"Critical_Error_Flag",
         "value":0
      }
   ]'

declare @parameters nvarchar(max) = '[
      {
         "name":"TPA_Name",
         "value":[
            "ACM",
            "Adair Horne",
            "Advent",
            "Ambridge",
            "Catalytic Claims Services",
            "Crawford & Company (UK)",
            "Crawford & Company (US)",
            "Davies Property Claims LLC",
            "DWF",
            "Gulf Coast Claims Service",
            "Hausch",
            "Johns Eastern",
            "Johnson & Johnson",
            "Mills Mehr",
            "Minuteman",
            "Minuteman (Ignore)",
            "NARS",
            "Peninsula Insurance Bureau",
            "Proctor",
            "Riverstone",
            "RRM",
            "SCM IPG",
            "Sedgwick",
            "State Street",
            "TestTPA",
            "Trident",
            "US IPG",
            "Vanguard",
            "Vanguard (Atrium)"
         ]
      },
      {
         "name":"Reporting_Period_End_Date",
         "value":[
            "05-31-2023",
            "06-30-2023",
            "07-31-2023"
         ]
      }
   ]'

declare @columns nvarchar(max) = '"Coverholder_Name",
      "TPA_Name",
      "Agreement",
      "Unique_Market_Reference_UMR",
      "Binder_Contract_Inception",
      "Binder_Contract_Expiry",
      "Reporting_Period_End_Date",
      "Class_of_Business",
      "Risk_Code",
      "Section_No",
      "Original_Currency",
      "Settlement_Currency",
      "Rate_of_Exchange",
      "Certificate_Reference",
      "Claim_Reference",
      "Insured_Full_Name",
      "Insured_State",
      "Insured_Country",
      "Location_of_Risk_State",
      "Location_of_Risk_County",
      "Risk_Inception_Date",
      "Risk_Expiry_Date",
      "Period_of_Cover",
      "Loss_State",
      "Location_of_Loss_Country",
      "Cause_of_Loss",
      "Loss_Description",
      "Date_of_Loss_From",
      "Date_Claim_Made",
      "Claim_Status",
      "Refer_to_Underwriters",
      "Denial",
      "Litigation_status",
      "Claimant_Name",
      "Loss_County",
      "State_of_Filing",
      "PCS_Code",
      "Medicare_United_States_Bodily_Injury",
      "Medicare_Eligibility_Check_Performed",
      "Medicare_Outcome_of_Eligibility_Status_Check",
      "Medicare_Conditional_Payments",
      "Medicare_MSP_Compliance_Services",
      "Paid_This_Month_Indemnity",
      "Paid_This_Month_Fees",
      "Previously_Paid_Indemnity",
      "Previously_Paid_Fees",
      "Reserve_Indemnity",
      "Reserve_Fees",
      "Change_This_Month_Indemnity",
      "Change_This_Month_Fees",
      "Total_Incurred_Indemnity",
      "Total_Incurred_Fees",
      "Coverholder_PIN",
      "Reporting_Period_Start_Date",
      "Type_of_Insurance",
      "Policy_or_Group_Ref",
      "Insured_Address",
      "Insured_Postcode_Zip_Code_or_similar",
      "Location_of_Risk_Location_ID",
      "Location_of_Risk_Address",
      "Location_of_Risk_Postcode_Zip_Code_or_similar",
      "Deductible_Amount",
      "Deductible_Basis",
      "Sums_Insured_Amount",
      "Location_of_Loss_Address",
      "Location_of_Loss_Postcode_Zip_Code_or_similar",
      "Date_Closed",
      "Lloyds_Cat_Code",
      "Catastrophe_Name",
      "Paid_this_month_Expenses",
      "Paid_this_month_Attorney_Coverage_Fees",
      "Paid_this_month_Adjusters_Fees",
      "Paid_this_month_Defence_Fees",
      "Paid_this_month_TPA_Fees",
      "Paid_this_month_Bank_Charges",
      "Previously_Paid_Expenses",
      "Previously_Paid_Attorney_Coverage_Fees",
      "Previously_Paid_Adjusters_Fees",
      "Previously_Paid_Defence_Fees",
      "Previously_Paid_TPA_Fees",
      "Previously_Paid_Bank_Charges",
      "Reserve_Expenses",
      "Reserve_Attorney_Coverage_Fees",
      "Reserve_Adjusters_Fees",
      "Reserve_Defence_Fees",
      "Reserve_TPA_Fees",
      "Reclosed_Date",
      "Net_Recovery",
      "Total_Incurred",
      "Expert_Role",
      "Expert_Firm",
      "Expert_Reference_No",
      "Expert_Address",
      "Expert_State",
      "Expert_Postcode_Zip_Code_or_similar",
      "Expert_Country",
      "Notes",
      "Date_Claim_Opened",
      "Ex_gratia_payment",
      "Claim_First_Notification_Acknowledgement_Date",
      "Date_First_Reserve_Established",
      "Date_Coverage_Confirmed",
      "Diary_date",
      "Peer_review_date",
      "Date_Claim_Amount_Agreed",
      "Date_Claims_Paid",
      "Date_of_Subrogation",
      "Date_Reopened",
      "Date_Claim_Denied",
      "Reason_for_Denial",
      "Date_claim_withdrawn",
      "Date_Coverage_Confirmed",
      "Subrogation_Recovered_This_Month",
      "Subrogation_Previously_Recovered",
      "Total_Subrogation_Recovered",
      "Salvage_Recovered_This_Month",
      "Salvage_Previously_Recovered",
      "Total_Salvage_Recovered",
      "Deductible_Recovered_This_Month",
      "Deductible_Previously_Recovered",
      "Total_Deductible_Recovered",
      "Gross_Recovery_Received_This_Month",
      "Gross_Recovery_Previously_Received",
      "Gross_Recovery_Total",
      "Recovery_Fees_Paid_This_Month",
      "Recovery_Fees_Paid_Previously_Paid",
      "Recovery_Fees_Total",
      "Driver_Age",
      "Cargo_Hauled",
      "Reefer_Claim",
      "Reefer_Age",
      "Cargo_Total_Insured_Value",
      "Vehicle_Unit_Age",
      "Vehicles_Total_Insured_Value",
      "Towing_Storage_Fees",
      "Trailer_Interchange",
      "Non_Owned_Trailer",
      "MTC_Peril",
      "Fault",
      "Distance",
      "Year_of_Account"'

declare @sql nvarchar(max)
declare @role varchar(20)


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


drop table if exists #tpa

select 
	[name]
	,ltrim(rtrim(replace(replace(replace(replace(s.[value], char(10), char(32)),char(13), char(32)),char(160), char(32)),char(9),char(32)))) as [TPA]
into #tpa
from #parameters p
cross apply string_split(trim('[""] ' from replace(p.[value],'"','')), ',') s
where [name] = 'TPA_Name'


declare @rlsWhereClause varchar(1000)
if (select count(*) from #tpa) > 0
	set @role = 'TPA'
	set @rlsWhereClause = 'select [TPA] from #tpa'
	
-- to be added: other roles than TPA



----- get flag parameter and prepare where clause

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


------ prepare columns, so they can be used inside sql query

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





-- 1) find the matching Group to the value

drop table if exists ##rlsIds

set @sql = 'select * into ##rlsIds
			from [dbo].[rls_filterset_rls_' + @role + ']
			where [parent_id] in (
				select [id] from [dbo].[rls_filterset_rls_' + @role + '] where ' + @role + ' in (' + @rlsWhereClause + ') and [parent_id] is null
	)
'
exec(@sql)

--select * from ##rlsIds

-- 2) Get UMRs for specified group
drop table if exists ##umr

set @sql = 'select distinct d.[UMR]
			into ##umr
			from [dbo].[rls_filterset_umr_' + @role + '] d
			join ##rlsIds r on r.[id] = d.[RLS_'+ @role + '_id]'

exec(@sql)


--select * from ##umr


-- 3) Get active umrs
drop table if exists #activeUmr

select distinct
	g.[umr]
	--,g.[umr_rc_cc_sn]
into #activeUmr
from [dbo].[rls_filterset_globalumr] g
join ##umr u on u.[UMR] = g.[umr]
where g.[bdx_status] = 1

--select * from #activeUmr

-- 4) Get rows from datarows table (based on user's columns and flags selection)

set @sql = 'select d.' + @columns + '
			from [dbo].[rig_datarows] d
			join #activeUmr a on a.[umr] = d.[Unique_Market_Reference_UMR]
			join [dbo].[rig_dataintegrityrules] dir on dir.[data_row_id] = d.[id] '
			+ @flagsWhereClause
			+ @reportingPeriodWhereClause

--print cast(substring(@sql, 1, 16000) as ntext )
--print cast(substring(@sql, 16001, 32000) as ntext )

exec(@sql)

end
GO


