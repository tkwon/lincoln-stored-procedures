create or alter procedure [export].[sp_GlobalUmrFilter]
	 @umr_criteria varchar(20) 
	,@fcp_status varchar(20)
	,@reserve varchar(20)
	,@claim_status varchar(20)
	,@page int
	,@pageSize int
	,@role varchar(20)
	,@role_value varchar(255)
	,@search varchar(50)
as

begin

declare @sql nvarchar(max)
declare @v_umr_criteria varchar(20)
declare @v_fcp_status varchar(20)
declare @v_reserve varchar(20)
declare @v_claim_status varchar(20)

declare @v_pageOffset varchar(5) = (select cast(@pageSize*(@page - 1) as varchar(10)))
declare @v_pageSize varchar(5) = (select cast(@pageSize as varchar(10)))

set @v_umr_criteria = case @umr_criteria
						when 'Active' then '1'
						when 'Inactive' then '0'
						when 'All' then '0,1'
					  end
			  
set @v_fcp_status = case @fcp_status
						when 'Active' then '1'
						when 'Inactive' then '0'
						when 'All' then '0,1'
					  end

set @v_reserve = case @reserve
					when 'True' then '1'
					else '0'
				 end

set @v_claim_status = case @claim_status
					when 'True' then '1'
					else '0'
				 end


if object_id('tempdb..#dataRows') is not null drop table #dataRows

select distinct
	[UMR_Risk_Cat_Section]
   ,case
		when [Reserve_Indemnity] = 0
			and [Reserve_Fees] = 0
			and [Reserve_Expenses] = 0
			and [Reserve_Attorney_Coverage_Fees] = 0
			and [Reserve_Adjusters_Fees] = 0
			and [Reserve_TPA_Fees] = 0
		then 0
		else 1
	end as [ReserveFlag]
	,case
		when [Claim_Status] in ('c', 'cl', 'clre', 'closed', 're-closed', 'withdrawn', 'denied') then 0
		else 1
	 end as [ClaimStatusFlag]
into #dataRows
from [dbo].[rig_datarows]

if object_id('tempdb..##finalResult') is not null drop table ##finalResult

set @sql = '
select
	[id]
	,[created_on]
	,[modified_on]
	,[umr]
	,[bdx_status]
	,[fcp_status]
	,[risk_code]
	,[section_no]
	,[cat_code]
	,[umr_rc_cc_sn]
	,[umr_rc_sn]
	,[billing_status]
	,[currency_type]
	,[fcp_start_date]
into  ##finalResult
from (
	select
		[id]
		,[created_on]
		,[modified_on]
		,[umr]
		,[bdx_status]
		,[fcp_status]
		,[risk_code]
		,[section_no]
		,[cat_code]
		,[umr_rc_cc_sn]
		,[umr_rc_sn]
		,[billing_status]
		,[currency_type]
		,[fcp_start_date]
		,row_number() over(order by [umr],[created_on]) as [rowN]
	from (
		select 
			g.[id]
			,g.[created_on]
			,g.[modified_on]
			,g.[umr]
			,g.[bdx_status]
			,g.[fcp_status]
			,g.[risk_code]
			,g.[section_no]
			,g.[cat_code]
			,g.[umr_rc_cc_sn]
			,g.[umr_rc_sn]
			,g.[billing_status]
			,g.[currency_type]
			,g.[fcp_start_date]
			,row_number() over(partition by g.[id] order by g.[id]) as [rowN]
		from [dbo].[rls_filterset_globalumr] g
		join #dataRows d on d.[UMR_Risk_Cat_Section] = g.[umr_rc_cc_sn]
		where g.[bdx_status] in (' + @v_umr_criteria + ') 
			and g.[fcp_status] in (' + @v_fcp_status + ') 
			and d.[ClaimStatusFlag] in (' + @v_claim_status + ') 
			and d.[ReserveFlag] in (' + @v_reserve + ') 
			and (g.[umr] like ''%' + @search + '%''
				or g.[risk_code] like ''%' + @search + '%''
				or g.[section_no] like ''%' + @search + '%''
				or g.[cat_code] like ''%' + @search + '%'')
	) w
	where [rowN] = 1
) ww
order by [rowN]
offset ' + @v_pageOffset + ' rows
fetch next ' + @v_pageSize + ' rows only'

exec sp_executesql @sql

select * from  ##finalResult

end