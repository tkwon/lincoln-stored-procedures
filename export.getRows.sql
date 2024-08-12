
create procedure [export].[sp_getData]

as

begin

-- declare variables which will be passed later as procedure parameters

declare @sql varchar(1000)
declare @role varchar(20) = 'TPA'
declare @roleValue varchar(100) = 'Hausch'
declare @flags varchar(50) = 'Non_Critical_Flag'
declare @columns varchar(2000) = 'id, processed_file_name, status, Claim_Reference, TPA_Name, Reporting_Period_End_Date, Policy_or_Group_Ref, Insured_Full_Name'

-- 1) find the matching Group to the value

drop table if exists ##rlsIds

set @sql = 'select * into ##rlsIds
			from [dbo].[rls_filterset_rls_' + @role + ']
			where [parent_id] in (
				select [id] from [dbo].[rls_filterset_rls_' + @role + '] where ' + @role + '= ''' + @roleValue + ''' and [parent_id] is null
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
			join [dbo].[rig_dataintegrityrules] dir on dir.[data_row_id] = d.[id]
			where dir.[' + @flags + '] = 1'

exec(@sql)

end
GO


