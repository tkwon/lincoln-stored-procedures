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
    ,@sort varchar(50)
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
                    when 'False' then '0'
                    when 'All' then '0,1'
                 end
set @v_claim_status = case @claim_status
                    when 'True' then '1'
                    when 'False' then '0'
                    when 'All' then '0,1'
                 end
---- Role handling -----
drop table if exists ##roleValues 
select
    [value]
into ##roleValues
from string_split(@role_value, ',')
declare @roleValueWhereClause varchar(255) = ''
declare @roleUmrJoin varchar(255) = ''
if @role <> 'admin'
begin
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
        where t2.[' + @role + '] in (select [value] from ##roleValues)
    ) w
    where [rowN] = 1'
    
    exec(@sql)
    set @roleUmrJoin = 'join ##roleUmrFiltered r on r.[UMR_Risk_Section] = d.[UMR_Risk_Section]'
end
if object_id('tempdb..##dataRows') is not null drop table ##dataRows
set @sql = 'select distinct
    d.[UMR_Risk_Cat_Section]
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
        when [Claim_Status] in (''c'', ''cl'', ''clre'', ''closed'', ''re-closed'', ''withdrawn'', ''denied'') then 0
        else 1
     end as [ClaimStatusFlag]
into ##dataRows
from [dbo].[rig_datarows] d
'
+ @roleUmrJoin
if object_id('tempdb..##tempGlobalUmr') is not null drop table ##tempGlobalUmr
exec sp_executesql @sql
set @sql = 'select
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
        ,[reserve_status]
        ,[claim_status]
        --,row_number() over(order by [umr],[created_on],[umr]) as [rowN]
    into ##tempGlobalUmr
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
            ,d.[ReserveFlag] as [reserve_status]
            ,d.[ClaimStatusFlag] as [claim_status]
            ,row_number() over(partition by g.[id] order by g.[id]) as [rowN]
        from [dbo].[rls_filterset_globalumr] g
        join ##dataRows d on d.[UMR_Risk_Cat_Section] = g.[umr_rc_cc_sn]
        where g.[bdx_status] in (' + @v_umr_criteria + ') 
            and g.[fcp_status] in (' + @v_fcp_status + ') 
            and d.[ClaimStatusFlag] in (' + @v_claim_status + ') 
            and d.[ReserveFlag] in (' + @v_reserve + ') 
            and (g.[umr] like ''%' + @search + '%''
                or g.[risk_code] like ''%' + @search + '%''
                or g.[section_no] like ''%' + @search + '%''
                or g.[cat_code] like ''%' + @search + '%'')
    ) w
    where [rowN] = 1'
exec sp_executesql @sql
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
    ,[reserve_status]
    ,[claim_status]
into  ##finalResult
from ##tempGlobalUmr
order by ' + @sort + '
offset ' + @v_pageOffset + ' rows
fetch next ' + @v_pageSize + ' rows only'
exec sp_executesql @sql
select count(*) as TotalRecords from ##tempGlobalUmr 
select * from  ##finalResult
end