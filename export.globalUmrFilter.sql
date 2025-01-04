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

declare @newId varchar(50) = (select replace(newid(),'-',''))

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
drop table if exists #roleValues 

select
    [value]
into #roleValues
from string_split(@role_value, ',')

declare @roleValueWhereClause varchar(255) = ''
declare @roleUmrJoin varchar(255) = ''

if @role <> 'admin'
begin
    drop table if exists #roleFiltered
    
    set @sql = 'select '
        + @role + '
    into #roleFiltered
    from (
        select 
            t1.[' + @role + ']
            ,row_number() over(partition by t2.[' + @role + '] order by t2.[' + @role + ']) as [rowN]
        from [rls_filterset_umr_' + @role + '] umr
        join [rls_filterset_rls_' + @role + '] t1 on umr.[RLS_' + @role + '_id] = t1.[id]
        join [rls_filterset_rls_' + @role + '] t2 on t1.[parent_id] = t2.[id]
        where t2.[' + @role + '] in (select [value] from #roleValues)
    ) w
    where [rowN] = 1'
    
    exec(@sql)

    set @roleValueWhereClause = ' where d.[' + @role + '_Name] in (select ' + @role + ' from #roleFiltered)'

	declare @t_roleUmrFiltered varchar(75) = '##roleUmrFiltered' + @newId
	set @sql = 'drop table if exists ' + @t_roleUmrFiltered

    exec(@sql)

    set @sql = 'select 
        [UMR_Risk_Section]
    into ' + @t_roleUmrFiltered + '
    from (
        select 
            umr.[UMR_Risk_Section]
            ,row_number() over(partition by umr.[UMR_Risk_Section] order by umr.[UMR_Risk_Section]) as [rowN]
        from [rls_filterset_umr_' + @role + '] umr
        join [rls_filterset_rls_' + @role + '] t1 on umr.[RLS_' + @role + '_id] = t1.[id]
        join [rls_filterset_rls_' + @role + '] t2 on t1.[parent_id] = t2.[id]
        where t2.[' + @role + '] in (select [value] from #roleValues)
    ) w
    where [rowN] = 1'
    
    exec(@sql)
    set @roleUmrJoin = 'join ' + @t_roleUmrFiltered + ' r on r.[UMR_Risk_Section] = d.[UMR_Risk_Section]'
end


declare @t_dataRows varchar(75) = '##dataRows' + @newId
set @sql = 'drop table if exists ' + @t_dataRows

exec(@sql)

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
into ' + @t_dataRows + '
from [dbo].[rig_datarows] d
' + @roleUmrJoin

exec sp_executesql @sql

declare @t_tempGlobalUmr varchar(75) = '##tempGlobalUmr' + @newId
set @sql = 'drop table if exists ' + @t_tempGlobalUmr

exec(@sql)

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
    into ' + @t_tempGlobalUmr + '
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
        join ' + @t_dataRows + ' d on d.[UMR_Risk_Cat_Section] = g.[umr_rc_cc_sn]
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

drop table if exists #finalResult

create table #finalResult (
     [id] int null
    ,[created_on] datetime2(7) null
    ,[modified_on] datetime2(7) null
    ,[umr] nvarchar(128)
    ,[bdx_status] bit null
    ,[fcp_status] bit null
    ,[risk_code] nvarchar(64) null
    ,[section_no] nvarchar(64) null
    ,[cat_code] nvarchar(64) null
    ,[umr_rc_cc_sn] nvarchar(350) null
    ,[umr_rc_sn] nvarchar(256) null
    ,[billing_status] bit null
    ,[currency_type] nvarchar(50) null
    ,[fcp_start_date] date null
    ,[reserve_status] bit null
    ,[claim_status]	bit null
)

set @sql = '
insert #finalResult ([id],[created_on],[modified_on],[umr],[bdx_status],[fcp_status],[risk_code],[section_no],[cat_code],[umr_rc_cc_sn],[umr_rc_sn],[billing_status],[currency_type],[fcp_start_date],[reserve_status],[claim_status])
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
from ' + @t_tempGlobalUmr + '
order by ' + @sort + '
offset ' + @v_pageOffset + ' rows
fetch next ' + @v_pageSize + ' rows only'

exec sp_executesql @sql

drop table if exists #count
create table #count (
	[TotalRecords] int null
)
set @sql = 'insert #count ([TotalRecords]) select count(*) from ' + @t_tempGlobalUmr
exec(@sql)

select [TotalRecords] from #count
select * from  #finalResult
end