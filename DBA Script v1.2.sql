/* ************************************************************************************************************* 
 #    Name:             DBA Script v1.2.sql
 #    Created:          11/03/2014
 #    RDBMS:            MSSQL
 #    Version:			1.2
 #    Summary: 
 #	   
 #	  Updates:			
 #						11/04/2014: Checks added to account for functionality not available in various versions of MSSQL (Specifically 2005 and 2008R2 SP1)
 #						11/12/2014: Added advanced config section for future use. (WIP)
 #									Added in @BufferSpecific to highlight which objects and number of pages are in buffer for @DB
 #
 #
 # ************************************************************************************************************* */

 /* ***************************************** Content Sources ************************************************** */
 /*  Much of the content below is modifications on free tools provided by SQL Skills and Brent Ozar Unlimited.   */
 /*  http://www.sqlskills.com/blogs/glenn/																		 */
 /*  http://www.brentozar.com/first-aid/																	     */
 
/* ***************************************** GLOBAL VARIABLES ************************************************** */
DECLARE @DB sysname, @infoGEN VARCHAR(5), @WAITSTATS VARCHAR(5), @waitsCLEAR VARCHAR(5), @infoBAK VARCHAR(5), @infoIO VARCHAR(5), @infoCPU VARCHAR(5), @infoMEM VARCHAR(5), @infoJOBS VARCHAR(5), 
@infoSEC VARCHAR(5), @infoFRAG VARCHAR(5), @infoSTAT VARCHAR(5), @Inventory VARCHAR(5), @showTableSizes VARCHAR(5), @Version as VARCHAR(128), @VersionMajor DECIMAL(10,2), @VersionMinor DECIMAL(10,2)

/* ******************************************* CONFIGURATION *************************************************** */

SET @DB   		= 'INOW6';				-- Name of database to review

SET @Inventory	= 'FALSE';				-- TRUE / FALSE

SET @infoGEN 	= 'FALSE';				-- TRUE / FALSE
	SET @showTableSizes = 'FALSE';			-- TRUE / FALSE: Set to TRUE if you want data returned for table data and index sizing

SET @WAITSTATS	= 'FALSE';				-- TRUE / FALSE   
	SET @waitsCLEAR = 'FALSE';      		-- TRUE / FALSE: Set to TRUE if you want to clear stored wait info. Will wait 10 seconds after clear before running waits.

SET @infoBAK 	= 'FALSE';				-- TRUE / FALSE

SET @infoIO  	= 'FALSE';				-- TRUE / FALSE

SET @infoCPU 	= 'FALSE';				-- TRUE / FALSE

SET @infoMEM 	= 'FALSE';				-- TRUE / FALSE

SET @infoJOBS 	= 'FALSE';				-- TRUE / FALSE

SET @infoSEC 	= 'FALSE';				-- TRUE / FALSE

SET @infoFRAG 	= 'FALSE';				-- TRUE / FALSE: May take awhile to run

SET @infoSTAT 	= 'FALSE';				-- TRUE / FALSE

SET @Version 	= CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(128));  	

/* ************************************ CONFIGURATION EXPLANATIONS ********************************************* */

/* 
@Inventory provides the following information:
		@infoGEN
		@infoHIST
		@infoFRAG
		@infoSTAT
	This section is designed to replace the existing SQL Inventory Script

@infoGEN provides the following information:
		Machine Name, Instance Name, Edition, Product Version
		Recovery Model, Log size, Log utilization, Compatibility Mode
		Current Connections
		All enabled traceflags
		Windows OS information
		SQL Services information
		SQL NUMA information
		Hardware basics
		Model/manufacturer of server
		Processor description
		File names and paths
		Last DBCC CHECKDB
		
 @WAITSTATS
		Grabs wait stats as seen by MSSQL
		Full list of all wait stat values can be found at
		http://msdn.microsoft.com/en-us/library/ms179984
		
 @infoBAK provides the following information:
		Last MSSQL backup for database @DB [***NOTE***: if customer is using different software to backup the database, this will not be correct]
		Last 5 transaction log backups for @DB
		Backup throughput speed
		Backup history for database file growth
		Compression factor (if being used for backups)
		VLF Counts

 @infoIO provides the following information:
		I/O Utilization by database
		Average stalls (I/O bottlenecks)
		Missing indexes identifier (do NOT add indexes based on these recommendations, need to evaluate them through Perceptive Software Research and Development team)
		Autogrowth events

 @infoCPU provides the following information:
		CPU utilization for MSSQL in last 256 minutes
		Signal waits
		CPU utilization by database

 @infoMEM provides the following information:
		Page life expectancy (PLE)
		Memory grants outstanding
		OS memory
		Total buffer usage by database
		
 @infoJOBS provides the following information:
		failed jobs
		jobs currently running 

 @infoSEC provides the following information:
		Who has SYSADMIN role?
		Who has SecADMIN role?

 @infoFRAG provides the following information:
		Fragmentation Levels across @DB

 @infoSTAT provides the following information:
		Date statistics were last updated across @DB
*/

/* ************************************ Declare advanced variables *************************************** */

DECLARE @BufferSpecific VARCHAR(5)

/* *************************************** Advanced Config *********************************************** */
/* ********************* Leave default if you do not know what a variable is for ************************* */

	-- Under @infoMEM
	SET @BufferSpecific = 'FALSE';			-- TRUE / FALSE: Set to TRUE if you need more specific information on the exact objects in buffer. (Specific to @DB)
	
/* ************************************ ADV CONFIGURATION EXPLANATIONS *********************************** */
/*

@BufferSpecific provides the following information:
	Using @DB, this section will allow you to see the name of the objects in buffer pool and how many pages from that object are in memory.

*/

/* ************************************ And so it begins...  ********************************************* */

/* ************************************ Limitations Check  *********************************************** */

SET @VersionMajor = SUBSTRING(@Version, 1,CHARINDEX('.', @Version) + 1 )
SET @VersionMinor = PARSENAME(CONVERT(varchar(32), @Version), 2)

IF @VersionMajor IN ('8.00', '9.00', '10.00') BEGIN

	PRINT 'We have detected you are on a version of MSSQL that is not support by this script. Script requires MSSQL 2008R2 SP2 or newer editions.'
	RAISERROR ('WARNING:  Version of SQL Server is not compatible with this script.', 20, 1) WITH LOG;
END

IF @VersionMinor IN ('1600.1', '2500.0') BEGIN

	PRINT 'We have detected you are currently on MSSQL 2008R2 RTM or SP1, some functionality may not be available.'
	PRINT 'For full script fucntionality, please update to MSSQL 2008R2 SP2 or newer.'
END

/* ************************************ Start the core script ********************************************* */

PRINT CHAR(13)
PRINT 'Database Performance Data'
PRINT CHAR(13) + + CHAR(13);
PRINT '##########  Date Run #############'
PRINT 'Date Ran : ' + CONVERT(CHAR(19),GETDATE(), 100)

IF @Inventory = 'TRUE' AND @DB LIKE '%INOW%' BEGIN
	--Get Product Version (ImageNow specific) 	
	EXEC ('USE ' + @DB + '
	SELECT PRODUCT_VERSION, MOD_TIME from INUSER.IN_PRODUCT_MOD_HIST 
	ORDER BY MOD_TIME
	')

END

IF @infoGEN = 'TRUE' or @Inventory = 'TRUE' BEGIN
	
	-- Get selected server properties 
	PRINT '##########  Selected Server Properties #############'
	SELECT RTRIM(CAST (SERVERPROPERTY('MachineName') AS VARCHAR)) AS [MachineName], 
		RTRIM(CAST (SERVERPROPERTY('ServerName')AS VARCHAR(50))) AS [ServerName],  
		RTRIM(CAST (SERVERPROPERTY('InstanceName')AS VARCHAR)) AS [Instance], 
		RTRIM(CAST (SERVERPROPERTY('IsClustered')AS VARCHAR)) AS [IsClustered], 
		RTRIM(CAST (SERVERPROPERTY('ComputerNamePhysicalNetBIOS')AS VARCHAR)) AS [ComputerNamePhysicalNetBIOS], 
		RTRIM(CAST (SERVERPROPERTY('Edition')AS VARCHAR)) AS [Edition], 
		RTRIM(CAST (SERVERPROPERTY('ProductLevel')AS VARCHAR)) AS [ProductLevel], 
		RTRIM(CAST (SERVERPROPERTY('ProductVersion')AS VARCHAR)) AS [ProductVersion], 
		RTRIM(CAST (SERVERPROPERTY('ProcessID')AS VARCHAR)) AS [ProcessID]
		
	-- Recovery model, log reuse wait description, log file size, log usage size  
	-- and compatibility level for all databases on instance
	PRINT '##########  Recovery Model, Log Size/Usage, Compatibility Level, Database Settings #############'
	SELECT RTRIM(CAST (db.[name] AS VARCHAR)) AS [Database Name], 
		RTRIM(CAST (db.recovery_model_desc AS VARCHAR)) AS [Recovery Model], 
		RTRIM(CAST (db.state_desc AS VARCHAR)) AS [State Desc], 
		RTRIM(CAST (db.log_reuse_wait_desc AS VARCHAR)) AS [Log Reuse Wait Description], 
		RTRIM(CAST (CONVERT(DECIMAL(18,2), ls.cntr_value/1024.0) AS VARCHAR)) AS [Log Size (MB)], 
		RTRIM(CAST (CONVERT(DECIMAL(18,2), lu.cntr_value/1024.0) AS VARCHAR)) AS [Log Used (MB)],
		RTRIM(CAST (CAST(CAST(lu.cntr_value AS FLOAT) / CAST(ls.cntr_value AS FLOAT)AS DECIMAL(18,2)) * 100 AS VARCHAR)) AS [Log Used %], 
		RTRIM(CAST (db.[compatibility_level] AS VARCHAR)) AS [DB Compatibility Level], 
		RTRIM(CAST (db.page_verify_option_desc AS VARCHAR)) AS [Page Verify Option], 
		RTRIM(CAST (db.is_auto_create_stats_on AS VARCHAR)) AS [Auto Create Stats On], 
		RTRIM(CAST (db.is_auto_update_stats_on AS VARCHAR)) AS [Auto Update Stats On],
		RTRIM(CAST (db.is_auto_update_stats_async_on AS VARCHAR)) AS [Auto Update Stats Async On], 
		RTRIM(CAST (db.is_parameterization_forced AS VARCHAR)) AS [Forced Parameterization], 
		RTRIM(CAST (db.snapshot_isolation_state_desc AS VARCHAR)) AS [Snapshot Isolation Level], 
		RTRIM(CAST (db.is_read_committed_snapshot_on AS VARCHAR)) AS [Read Committed Snapshot],
		RTRIM(CAST (db.is_auto_close_on AS VARCHAR)) AS [Auto Close On], 
		RTRIM(CAST (db.is_auto_shrink_on AS VARCHAR)) AS [Auto Shrink On], 
		RTRIM(CAST (db.target_recovery_time_in_seconds AS VARCHAR)) AS [Target Recovery Time(S)], 
		RTRIM(CAST (db.is_cdc_enabled AS VARCHAR)) AS [CDC Enabled]
	FROM sys.databases AS db WITH (NOLOCK)
		INNER JOIN sys.dm_os_performance_counters AS lu WITH (NOLOCK)
		ON db.name = lu.instance_name
		INNER JOIN sys.dm_os_performance_counters AS ls WITH (NOLOCK)
		ON db.name = ls.instance_name
	WHERE lu.counter_name LIKE N'Log File(s) Used Size (KB)%' 
		AND ls.counter_name LIKE N'Log File(s) Size (KB)%'
		AND ls.cntr_value > 0 OPTION (RECOMPILE);

	--  Get logins that are connected and how many sessions they have
	PRINT '##########  Connection Information #############'
	SELECT RTRIM(CAST (login_name AS VARCHAR)) AS [Login Name], 
		RTRIM(CAST ([program_name] AS VARCHAR(50))) AS [Program Name], 
		COUNT(session_id) AS [session_count] 
	FROM sys.dm_exec_sessions WITH (NOLOCK)
		GROUP BY login_name, [program_name]
		ORDER BY COUNT(session_id) DESC OPTION (RECOMPILE);

	-- Returns a list of all global trace flags that are enabled 
	PRINT '##########  Global Trace Flags #############'
	DBCC TRACESTATUS (-1)
	
	-- Windows information 
	PRINT '##########  Windows Information #############'
	SELECT RTRIM(CAST (windows_release AS VARCHAR)) AS [Windows Release], 
		RTRIM(CAST (windows_service_pack_level AS VARCHAR)) AS [Service Pack Level], 
        RTRIM(CAST (windows_sku AS VARCHAR)) AS [Windows SKU], 
		RTRIM(CAST (os_language_version AS VARCHAR)) AS [Windows Language]
	FROM sys.dm_os_windows_info WITH (NOLOCK) OPTION (RECOMPILE);

	-- Gives you major OS version, Service Pack, Edition, and language info for the operating system 
	-- 6.3 is either Windows 8.1 or Windows Server 2012 R2
	-- 6.2 is either Windows 8 or Windows Server 2012
	-- 6.1 is either Windows 7 or Windows Server 2008 R2
	-- 6.0 is either Windows Vista or Windows Server 2008

	-- Windows SKU codes
	-- 4  is Enterprise Edition
	-- 48 is Professional Edition

	-- 1033 for os_language_version is US-English
	
	-- SQL Server Services information 
	PRINT '##########  SQL Services Information #############'
	SELECT RTRIM(CAST (servicename AS VARCHAR(50))) AS [Service Name], 
		RTRIM(CAST (process_id AS VARCHAR)) AS [Process ID],
		RTRIM(CAST (startup_type_desc AS VARCHAR)) AS [Startup Type], 
		RTRIM(CAST (status_desc AS VARCHAR)) AS [Status Description], 
		RTRIM(CAST (last_startup_time AS VARCHAR(50))) AS [Last Startup Time], 
		RTRIM(CAST (service_account AS VARCHAR(50))) AS [Service Account], 
		RTRIM(CAST (is_clustered AS VARCHAR)) AS [Is Clustered], 
		RTRIM(CAST (cluster_nodename AS VARCHAR)) AS [Cluster Node Name], 
		RTRIM(CAST ([filename] AS VARCHAR(100))) AS [Filename]
	FROM sys.dm_server_services WITH (NOLOCK) OPTION (RECOMPILE);

	-- SQL Server NUMA Node information  
	PRINT '##########  SQL Numa Information #############'
	SELECT RTRIM(CAST (node_id AS VARCHAR)) AS [Node ID], 
		RTRIM(CAST (node_state_desc AS VARCHAR)) AS [Node State Desc], 
		RTRIM(CAST (memory_node_id AS VARCHAR)) AS [Memory Node ID], 
		RTRIM(CAST (processor_group AS VARCHAR)) AS [Processor Group], 
		RTRIM(CAST (online_scheduler_count AS VARCHAR)) AS [(Online Schedulers], 
        RTRIM(CAST (active_worker_count AS VARCHAR)) AS [Active Workers], 
		RTRIM(CAST (avg_load_balance AS VARCHAR)) AS [Average Load Balance], 
		RTRIM(CAST (resource_monitor_state AS VARCHAR)) AS [Resource Monitor Stats]
	FROM sys.dm_os_nodes WITH (NOLOCK) 
	WHERE node_state_desc <> N'ONLINE DAC' OPTION (RECOMPILE);
	
	-- Hardware information from SQL Server 2012 
	-- (Cannot distinguish between HT and multi-core)
	PRINT '##########  Hardware Information #############'
	SELECT RTRIM(CAST (cpu_count AS VARCHAR)) AS [Logical CPU Count], 
		RTRIM(CAST (scheduler_count AS VARCHAR)) AS [Scheduler Count], 
		RTRIM(CAST (hyperthread_ratio AS VARCHAR)) AS [Hyperthread Ratio],
		RTRIM(CAST (cpu_count/hyperthread_ratio AS VARCHAR)) AS [Physical CPU Count], 
		RTRIM(CAST (physical_memory_kb/1024 AS VARCHAR)) AS [Physical Memory (MB)], 
		RTRIM(CAST (committed_kb/1024 AS VARCHAR)) AS [Committed Memory (MB)],
		RTRIM(CAST (committed_target_kb/1024 AS VARCHAR)) AS [Committed Target Memory (MB)],
		RTRIM(CAST (max_workers_count AS VARCHAR)) AS [Max Workers Count], 
		RTRIM(CAST (affinity_type_desc AS VARCHAR)) AS [Affinity Type], 
		RTRIM(CAST (sqlserver_start_time AS VARCHAR)) AS [SQL Server Start Time], 
		RTRIM(CAST (virtual_machine_type_desc AS VARCHAR)) AS [Virtual Machine Type]  
	FROM sys.dm_os_sys_info WITH (NOLOCK) OPTION (RECOMPILE);

	-- Get System Manufacturer and model number from
	PRINT '##########  Manufacturer Information #############'
	EXEC  xp_readerrorlog 0, 1, N'Manufacturer'; 

	-- Get processor description from Windows Registry 
	PRINT '##########  Processor Description #############'
	EXEC xp_instance_regread N'HKEY_LOCAL_MACHINE', N'HARDWARE\DESCRIPTION\System\CentralProcessor\0', N'ProcessorNameString';
	
	-- File names and paths for TempDB and all user databases in instance 
	PRINT '##########  File Names and Paths #############'
	SELECT RTRIM(CAST (DB_NAME([database_id]) AS VARCHAR)) AS [Database Name], 
       RTRIM(CAST ([file_id] AS VARCHAR)) AS [File ID], 
	   RTRIM(CAST (name AS VARCHAR)) AS [Name], 
	   RTRIM(CAST (physical_name AS VARCHAR(100))) AS [Physical Name], 
	   RTRIM(CAST (type_desc AS VARCHAR)) AS [Type Desc], 
	   RTRIM(CAST (state_desc AS VARCHAR)) AS [State Desc],
	   RTRIM(CAST (is_percent_growth AS VARCHAR)) AS [Is Percent Growth], 
	   RTRIM(CAST (growth AS VARCHAR)) AS [Growth],
	   RTRIM(CAST (CONVERT(bigint, growth/128.0) AS VARCHAR)) AS [Growth in MB], 
       RTRIM(CAST (CONVERT(bigint, size/128.0) AS VARCHAR)) AS [Total Size in MB]
	FROM sys.master_files WITH (NOLOCK)
	WHERE [database_id] > 4 
	AND [database_id] <> 32767
	OR [database_id] = 2
	ORDER BY DB_NAME([database_id]) OPTION (RECOMPILE);
	
	-- Last DBCC CheckDB execution
	PRINT '##########  Last CheckDB Run #############'
	CREATE TABLE #temp
		(
		  ParentObject VARCHAR(255) ,
		  [Object] VARCHAR(255) ,
		  Field VARCHAR(255) ,
		  [Value] VARCHAR(255)
		)   
	 
	CREATE TABLE #DBCCResults
		(
		  ServerName VARCHAR(255) ,
		  DBName VARCHAR(255) ,
		  LastCleanDBCCDate DATETIME
		)   
	 
	EXEC master.dbo.sp_MSforeachdb @command1 = 'USE [?]; INSERT INTO #temp EXECUTE (''DBCC DBINFO WITH TABLERESULTS'')',
		@command2 = 'INSERT INTO #DBCCResults SELECT @@SERVERNAME, ''?'', Value FROM #temp WHERE Field = ''dbi_dbccLastKnownGood''',
		@command3 = 'TRUNCATE TABLE #temp'   
	    --Delete duplicates due to a bug in SQL Server 2008
		;
	WITH    DBCC_CTE
			  AS ( SELECT   ROW_NUMBER() OVER ( PARTITION BY ServerName, DBName,
												LastCleanDBCCDate ORDER BY LastCleanDBCCDate ) RowID
				   FROM     #DBCCResults
				 )
		DELETE  FROM DBCC_CTE
		WHERE   RowID > 1 ;
	 
	SELECT  RTRIM(CAST (ServerName AS VARCHAR)) AS [Server Name] ,
			RTRIM(CAST (DBName AS VARCHAR)) AS [Database Name] ,
			CASE LastCleanDBCCDate
			  WHEN '1900-01-01 00:00:00.000' THEN 'Never ran DBCC CHECKDB'
			  ELSE CAST(LastCleanDBCCDate AS VARCHAR)
			END AS LastCleanDBCCDate
	FROM    #DBCCResults
	WHERE DBName = @DB
	ORDER BY 3
	 
	DROP TABLE #temp, #DBCCResults;

	-- Database/table sizing 
	CREATE TABLE #tempSizing(
		rec_id              int IDENTITY (1, 1),
		table_name  varchar(128),
		nbr_of_rows int,
		data_space  decimal(15,2),
		index_space decimal(15,2),
		total_size  decimal(15,2),
		percent_of_db       decimal(15,12),
		db_size             decimal(15,2))

		-- Get all tables, names, and sizes
		EXEC sp_msforeachtable @command1="insert into #tempSizing(nbr_of_rows, data_space, index_space) exec sp_mstablespace '?'",
							@command2="update #tempSizing set table_name = '?' where rec_id = (select max(rec_id) from #tempSizing)"

		-- Set the total_size and total database size fields
		UPDATE #tempSizing
		SET total_size = (data_space + index_space), db_size = (SELECT SUM(data_space + index_space) FROM #tempSizing)

		-- Set the percent of the total database size
		UPDATE #tempSizing
		SET percent_of_db = (total_size/db_size) * 100

		-- Get the data
		
		IF @showTableSizes = 'TRUE' BEGIN
			SELECT 
				  table_name [Table]
				, nbr_of_rows [Row Count]
				, data_space [Data size (kb)]
				, index_space [Index size (kb)]
				, total_size [Table size (kb)]
				, percent_of_db [% of DB size]
			FROM #tempSizing
			ORDER BY table_name
		END
		
		SELECT 
			  (SUM(total_size) / 1048576) [Database Used Size (GB)] 
			, (SUM(data_space) / 1048576)  [Data Space Used (GB)]
			, (SUM(index_space) / 1048576) [Index Space Used (GB)] 
			, (SUM(data_space) / SUM(total_size) * 100) [Data % of database]
			, (SUM(index_space) / SUM(total_size) * 100) [Index % Space Used]
		FROM #tempSizing

		DROP TABLE #tempSizing
		
		-- File size and % used 
		CREATE TABLE #dataSizing(
			file_name  varchar(128),
			file_space  decimal(15,2),
			consumed_space decimal(15,2),
			drive_space  decimal(15,2),
		)
		
		INSERT INTO #dataSizing (file_name, file_space, consumed_space, drive_space)
			SELECT TOP 3
				--BS.database_name
				  BF.physical_name
				, CAST(BF.file_size/1024/1024 AS decimal(15)) AS file_size_mb
				, CAST(BF.backup_size/1024/1024 AS decimal(15)) AS consumed_size_mb
				, '0'
			FROM msdb.dbo.backupset BS
				INNER JOIN msdb.dbo.backupfile BF ON BS.backup_set_id = BF.backup_set_id
			WHERE  BS.type = 'D'
			AND BS.database_name = 'INOW6' --@DB
			ORDER BY BS.database_name
				, BS.backup_finish_date DESC
				, BF.logical_name

		declare @Drive table(DriveName char, FreeSpaceInMegabytes int)
		insert @Drive execute xp_fixeddrives

		update #dataSizing SET drive_space = (
			select
				--mas.type_desc FileType, 
				--mas.name FileName, 
				--mas.physical_name PhysicalFileName, 
				--mas.size * 8 / 1024 FileSizeInMegabytes,
				--drv.DriveName, 
				drv.FreeSpaceInMegabytes
				--, CAST(BF.file_size/1024/1024 AS decimal(15)) AS file_size_mb
				--, CAST(BF.backup_size/1024/1024 AS decimal(15)) AS consumed_size_mb
			from sys.master_files mas
				left join @Drive drv on left(mas.physical_name, 1) = drv.DriveName
				left join msdb.dbo.backupfile bf on bf.physical_drive = drv.DriveName
			where database_id = db_id(@DB) and mas.physical_name = #dataSizing.file_name
		)
			
		SELECT
			  file_name [file]
			, (drive_space / 1024) [Available drive space (GB)]
			, (file_space / 1024) [File Size (GB)]
			, (consumed_space / 1024) [Consumed Size (GB)]
			, ((consumed_space / file_space) * 100) [% Used]
		FROM #dataSizing
		
		drop table #dataSizing;	
END

IF @WAITSTATS = 'TRUE' BEGIN

	IF @waitsCLEAR = 'TRUE' BEGIN
	-- Clear current wait stats
	PRINT '##########  Wait Stats Clear #############'
	DBCC SQLPERF('sys.dm_os_wait_stats', CLEAR);
	
	WAITFOR DELAY '00:00:10'
	
	END
	
	-- Clear Wait Stats 
	-- DBCC SQLPERF('sys.dm_os_wait_stats', CLEAR);

	-- Isolate top waits for server instance since last restart or statistics clear 
	
	-- Version of WAIT_STATS that supports 2008 R2 SP2
	IF @VersionMajor IN ('10.50') BEGIN
	PRINT '##########  Wait Stats 2008 #############';
	WITH    Waits
          AS (SELECT    wait_type
                       ,wait_time_ms / 1000. AS wait_time_s
                       ,100. * wait_time_ms / SUM(wait_time_ms) OVER () AS pct
                       ,ROW_NUMBER() OVER (ORDER BY wait_time_ms DESC) AS rn
              FROM     sys.dm_os_wait_stats
              WHERE    wait_type NOT IN (
                       N'CLR_SEMAPHORE',
                       N'LAZYWRITER_SLEEP',
                       N'RESOURCE_QUEUE',
                       N'SLEEP_TASK',
                       N'SLEEP_SYSTEMTASK',
                       N'SQLTRACE_BUFFER_FLUSH',
                       N'WAITFOR',
                       N'LOGMGR_QUEUE',
                       N'CHECKPOINT_QUEUE',
                       N'REQUEST_FOR_DEADLOCK_SEARCH',
                       N'XE_TIMER_EVENT',
                       N'BROKER_TO_FLUSH',
                       N'BROKER_TASK_STOP',
                       N'CLR_MANUAL_EVENT',
                       N'CLR_AUTO_EVENT',
                       N'DISPATCHER_QUEUE_SEMAPHORE',
                       N'FT_IFTS_SCHEDULER_IDLE_WAIT',
                       N'XE_DISPATCHER_WAIT',
                       N'XE_DISPATCHER_JOIN',
                       N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
                       N'ONDEMAND_TASK_QUEUE',
                       N'BROKER_EVENTHANDLER',
                       N'SLEEP_BPOOL_FLUSH',
					   N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
					   N'DIRTY_PAGE_POLL')
             )
     SELECT W1.wait_type
           ,CAST(W1.wait_time_s AS DECIMAL(12, 2)) AS wait_time_s
           ,CAST(W1.pct AS DECIMAL(12, 2)) AS pct
           ,CAST(SUM(W2.pct) AS DECIMAL(12, 2)) AS running_pct
     FROM   Waits AS W1
            INNER JOIN Waits AS W2 ON W2.rn <= W1.rn
     GROUP BY W1.rn
           ,W1.wait_type
           ,W1.wait_time_s
           ,W1.pct
     HAVING SUM(W2.pct) - W1.pct < 99
	OPTION  (RECOMPILE); -- percentage threshold
	
	END

	IF @VersionMajor IN ('11.00', '12.00') BEGIN 
	PRINT '##########  Wait Stats 2012+ #############';
	-- Updated WAIT_STATS query for 2012+
	
	WITH Waits
	AS (SELECT wait_type, CAST(wait_time_ms / 1000. AS DECIMAL(12, 2)) AS [wait_time_s],
		CAST(100. * wait_time_ms / SUM(wait_time_ms) OVER () AS decimal(12,2)) AS [pct],
		ROW_NUMBER() OVER (ORDER BY wait_time_ms DESC) AS rn
	FROM sys.dm_os_wait_stats WITH (NOLOCK)
	WHERE wait_type NOT IN (N'CLR_SEMAPHORE', 
							N'LAZYWRITER_SLEEP', 
							N'RESOURCE_QUEUE',
							N'SLEEP_TASK',
			                N'SLEEP_SYSTEMTASK', 
							N'SQLTRACE_BUFFER_FLUSH', 
							N'WAITFOR', N'LOGMGR_QUEUE',
			                N'CHECKPOINT_QUEUE', 
							N'REQUEST_FOR_DEADLOCK_SEARCH', 
							N'XE_TIMER_EVENT',
			                N'BROKER_TO_FLUSH', 
							N'BROKER_TASK_STOP', 
							N'CLR_MANUAL_EVENT', 
							N'CLR_AUTO_EVENT',
			                N'DISPATCHER_QUEUE_SEMAPHORE' ,
							N'FT_IFTS_SCHEDULER_IDLE_WAIT', 
							N'XE_DISPATCHER_WAIT',
			                N'XE_DISPATCHER_JOIN', 
							N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 
							N'ONDEMAND_TASK_QUEUE',
			                N'BROKER_EVENTHANDLER', 
							N'SLEEP_BPOOL_FLUSH', 
							N'SLEEP_DBSTARTUP', 
							N'DIRTY_PAGE_POLL',
			                N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
							N'SP_SERVER_DIAGNOSTICS_SLEEP')),
							Running_Waits 
	AS (SELECT W1.wait_type, wait_time_s, pct,
		SUM(pct) OVER(ORDER BY pct DESC ROWS UNBOUNDED PRECEDING) AS [running_pct]
	FROM Waits AS W1)
	SELECT wait_type, wait_time_s, pct, running_pct
	FROM Running_Waits
	WHERE running_pct - pct <= 99
		ORDER BY running_pct
		OPTION (RECOMPILE);
	
	END
END

IF @infoBAK = 'TRUE' BEGIN

	/* Last backup
			
	If physical_device_name is local to the server, make recommendation to move it to a network share.  This would provide several things:
		1.	Get the data off the server faster
		2.	Restore to dev/QA faster
		3.	Faster writes to tape
		
	Information taken from http://www.brentozar.com/archive/2008/09/back-up-your-database-to-the-network-not-local-disk/
	*/	
	PRINT '##########  Last Backup #############';
	WITH full_backups AS
	(
	SELECT
		ROW_NUMBER() OVER(PARTITION BY database_name ORDER BY database_name ASC, backup_finish_date DESC) AS [Row Number]
		, database_name
		, backup_set_id
		, backup_finish_date
	FROM msdb.dbo.[backupset]
	WHERE [type] = 'D'
	)
	 
	SELECT
		RTRIM(CAST (BS.server_name AS VARCHAR)) AS [Server Name], 
		RTRIM(CAST (BS.database_name AS VARCHAR)) AS [Database Name],
		RTRIM(CAST (FB.backup_finish_date AS VARCHAR)) AS [Backup Date], 
		RTRIM(CAST (BMF.physical_device_name AS VARCHAR)) AS [Physical Device Name], 
		RTRIM(CAST (BMF.logical_device_name AS VARCHAR)) AS [Logical Device Name]
	FROM full_backups FB
		INNER JOIN msdb.dbo.[backupset] BS ON FB.backup_set_id = BS.backup_set_id
		INNER JOIN msdb.dbo.backupmediafamily BMF ON BS.media_set_id = BMF.media_set_id
	WHERE FB.[Row Number] = 1
	AND BS.database_name = @DB 
	ORDER BY FB.database_name;

	/* Transaction log backup

	If physical_device_name is local to the server, make recommendation to move it to a network share.  This would provide several things:
		1.	Get the data off the server faster
		2.	Restore to dev/QA faster
		3.	Faster writes to tape
		
	Information taken from http://www.brentozar.com/archive/2008/09/back-up-your-database-to-the-network-not-local-disk/
	*/	
	PRINT '##########  Transaction Log Backup #############';
	WITH log_backups AS
	(
	SELECT
		ROW_NUMBER() OVER(PARTITION BY database_name ORDER BY database_name ASC, backup_finish_date DESC) AS [Row Number]
		, database_name
		, backup_set_id
		, backup_finish_date
	FROM msdb.dbo.[backupset]
	WHERE [type] = 'L'
	)
	 
	SELECT TOP 5
		RTRIM(CAST (BS.server_name AS VARCHAR)) AS [Server Name], 
		RTRIM(CAST (BS.database_name AS VARCHAR)) AS [Database Name],
		RTRIM(CAST (FB.backup_finish_date AS VARCHAR)) AS [Backup Date], 
		RTRIM(CAST (BMF.physical_device_name AS VARCHAR)) AS [Physical Device Name], 
		RTRIM(CAST (BMF.logical_device_name AS VARCHAR)) AS [Logical Device Name]
	FROM log_backups FB
	 INNER JOIN msdb.dbo.[backupset] BS ON FB.backup_set_id = BS.backup_set_id
	 INNER JOIN msdb.dbo.backupmediafamily BMF ON BS.media_set_id = BMF.media_set_id
	WHERE BS.database_name = @DB
	ORDER BY bs.database_name, FB.backup_finish_date DESC;

	-- If throughput changes, ask the SAN team what changed.  
	PRINT '##########  Backup Throughput #############'
	SELECT  RTRIM(CAST (@@SERVERNAME AS VARCHAR)) AS [Server Name] ,
			YEAR(backup_finish_date) AS backup_year ,
			MONTH(backup_finish_date) AS backup_month ,
			CAST(AVG(( backup_size / ( DATEDIFF(ss, bset.backup_start_date,
												bset.backup_finish_date) )
					   / 1048576 )) AS INT) AS throughput_MB_sec_avg ,
			CAST(MIN(( backup_size / ( DATEDIFF(ss, bset.backup_start_date,
												bset.backup_finish_date) )
					   / 1048576 )) AS INT) AS throughput_MB_sec_min ,
			CAST(MAX(( backup_size / ( DATEDIFF(ss, bset.backup_start_date,
												bset.backup_finish_date) )
					   / 1048576 )) AS INT) AS throughput_MB_sec_max
	FROM    msdb.dbo.backupset bset
	WHERE   bset.type = 'D' -- full backups only 
			AND bset.backup_size > 5368709120 -- 5GB or larger 
			AND DATEDIFF(ss, bset.backup_start_date, bset.backup_finish_date) > 1 -- backups lasting over a second
	GROUP BY YEAR(backup_finish_date) ,
			MONTH(backup_finish_date)
	ORDER BY @@SERVERNAME ,
			YEAR(backup_finish_date) DESC ,
			MONTH(backup_finish_date) DESC
		 	
	-- Use Backup History to Examine Database File Growth 
	PRINT '##########  Database file Growth #############';
	SELECT 
		RTRIM(CAST (BS.database_name AS VARCHAR)) AS [Database Name]
		,RTRIM(CAST(BF.file_size/1024/1024 AS bigint)) AS file_size_mb
		,RTRIM(CAST(BF.backup_size/1024/1024 AS bigint)) AS consumed_size_mb
		,RTRIM(CAST (BF.logical_name AS VARCHAR)) AS [Logical Name]
		,RTRIM(CAST (BF.physical_name AS VARCHAR)) AS [Physical Name]
		,RTRIM(CAST (BS.backup_finish_date AS VARCHAR)) AS polling_date
	FROM msdb.dbo.backupset BS
		INNER JOIN msdb.dbo.backupfile BF ON BS.backup_set_id = BF.backup_set_id
	WHERE  BS.type = 'D'
	AND BS.database_name = @DB
	ORDER BY BS.database_name
		, BS.backup_finish_date DESC
		, BF.logical_name;

	-- Determine Compression Factor for Individual Databases 
	PRINT '##########  Compression Ratios #############';
	WITH full_backups AS
	(
	SELECT
		ROW_NUMBER() OVER(PARTITION BY BS.database_name ORDER BY BS.database_name ASC, BS.backup_finish_date DESC) AS [Row Number]
		, BS.database_name
		, BS.backup_set_id
		, BS.backup_size AS uncompressed_size
		, BS.backup_finish_date
	FROM msdb.dbo.[backupset] BS
	WHERE BS.[type] = 'D'
	)
	 
	SELECT
		RTRIM(CAST (FB.database_name AS VARCHAR)) AS [Database Name]
		, RTRIM(CAST (FB.backup_finish_date AS VARCHAR)) AS [Backup Finish Date]
		, RTRIM(CAST (FB.uncompressed_size AS VARCHAR)) AS [Uncompressed Size]
	FROM full_backups FB
	 INNER JOIN msdb.dbo.[backupset] BS ON FB.backup_set_id = BS.backup_set_id
	 INNER JOIN msdb.dbo.backupmediafamily BMF ON BS.media_set_id = BMF.media_set_id
	WHERE FB.[Row Number] = 1
	AND BS.database_name = @DB
	ORDER BY FB.database_name;
	
	-- Get VLF Counts for all databases on the instance
	PRINT '##########  VLF Counts #############';
	CREATE TABLE #VLFInfo (RecoveryUnitID int, FileID  int,
					   FileSize bigint, StartOffset bigint,
					   FSeqNo      bigint, [Status]    bigint,
					   Parity      bigint, CreateLSN   numeric(38));
	 
	CREATE TABLE #VLFCountResults(DatabaseName sysname, VLFCount int);
	 
	EXEC sp_MSforeachdb N'Use [?]; 

				INSERT INTO #VLFInfo 
				EXEC sp_executesql N''DBCC LOGINFO([?])''; 
	 
				INSERT INTO #VLFCountResults 
				SELECT DB_NAME(), COUNT(*) 
				FROM #VLFInfo; 

				TRUNCATE TABLE #VLFInfo;'
	 
	SELECT RTRIM(CAST (DatabaseName AS VARCHAR)) AS [Database Name], 
		RTRIM(CAST (VLFCount AS VARCHAR)) AS [VLF Count] 
	FROM #VLFCountResults
		ORDER BY VLFCount DESC;
	 
	DROP TABLE #VLFInfo;
	DROP TABLE #VLFCountResults;
	
END

IF @infoIO = 'TRUE' BEGIN
		-- Get I/O utilization by database
	PRINT '##########  IO Utilization by Database #############';
	WITH Aggregate_IO_Statistics
	AS
	(SELECT DB_NAME(database_id) AS [Database Name],
	CAST(SUM(num_of_bytes_read + num_of_bytes_written)/1048576 AS DECIMAL(12, 2)) AS io_in_mb
	FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS [DM_IO_STATS]
	GROUP BY database_id)
	SELECT RTRIM(CAST (ROW_NUMBER() OVER(ORDER BY io_in_mb DESC)AS VARCHAR)) AS [I/O Rank], 
		RTRIM(CAST ([Database Name] AS VARCHAR)) AS [Database Name], 
		RTRIM(CAST (io_in_mb AS VARCHAR)) AS [Total I/O (MB)],
		RTRIM(CAST (io_in_mb/ SUM(io_in_mb) OVER() * 100.0 AS DECIMAL(5,2))) AS [I/O Percent]
	FROM Aggregate_IO_Statistics
	ORDER BY [I/O Rank] OPTION (RECOMPILE);
	
	-- Calculates average stalls per read, per write, and per total input/output for each database file. 
	PRINT '##########  Stall Information #############';
	SELECT RTRIM(CAST (DB_NAME(fs.database_id) AS VARCHAR)) AS [Database Name], 
		RTRIM(CAST (mf.physical_name AS VARCHAR(100))) AS [Physical Name], 
		RTRIM(CAST (io_stall_read_ms AS VARCHAR)) AS [IO Stall Read ms], 
		RTRIM(CAST (num_of_reads AS VARCHAR)) AS [Number of Reads],
		RTRIM(CAST(io_stall_read_ms/(1.0 + num_of_reads) AS NUMERIC(10,1))) AS [Avg Read Stall ms],
		RTRIM(CAST (io_stall_write_ms AS VARCHAR)) AS [IO Stall Write ms], 
		RTRIM(CAST (num_of_writes AS VARCHAR)) AS [Number of Writes],
		RTRIM(CAST(io_stall_write_ms/(1.0+num_of_writes) AS NUMERIC(10,1))) AS [Avg Write Stall ms],
		RTRIM(CAST (io_stall_read_ms + io_stall_write_ms AS VARCHAR)) AS [IO Stalls], 
		RTRIM(CAST (num_of_reads + num_of_writes AS VARCHAR)) AS [Total IO],
		RTRIM(CAST((io_stall_read_ms + io_stall_write_ms)/(1.0 + num_of_reads + num_of_writes) AS NUMERIC(10,1))) AS [Avg IO Stall ms]
	FROM sys.dm_io_virtual_file_stats(null,null) AS fs
	INNER JOIN sys.master_files AS mf
	ON fs.database_id = mf.database_id
	AND fs.[file_id] = mf.[file_id]
	ORDER BY io_stall_read_ms + io_stall_write_ms DESC OPTION (RECOMPILE);
	-- Helps you determine which database files on the entire instance have the most I/O bottlenecks
	
	-- Missing Indexes current database by Index Advantage
	PRINT '##########  Potential Index Additions #############';
	SELECT user_seeks * avg_total_user_cost * (avg_user_impact * 0.01) AS [index_advantage], 
		migs.last_user_seek, mid.[statement] AS [Database.Schema.Table],
		mid.equality_columns, mid.inequality_columns, mid.included_columns,
		migs.unique_compiles, migs.user_seeks, migs.avg_total_user_cost, migs.avg_user_impact
	FROM sys.dm_db_missing_index_group_stats AS migs WITH (NOLOCK)
	INNER JOIN sys.dm_db_missing_index_groups AS mig WITH (NOLOCK)
	ON migs.group_handle = mig.index_group_handle
	INNER JOIN sys.dm_db_missing_index_details AS mid WITH (NOLOCK)
	ON mig.index_handle = mid.index_handle
	WHERE mid.database_id = DB_ID() -- Remove this to see for entire instance
	ORDER BY index_advantage DESC OPTION (RECOMPILE);

	-- Look at index advantage, last user seek time, number of user seeks to help determine source and importance
	-- SQL Server is overly eager to add included columns, so beware
	-- Do not just blindly add indexes that show up from this query!!!

	--	Autogrowth events
	PRINT '##########  AutoGrowth Events #############';
	DECLARE @filename NVARCHAR(1000), @bc INT, @ec INT, @bfn VARCHAR(1000), @efn VARCHAR(10)
	SELECT  @filename = CAST(value AS NVARCHAR(1000))
	FROM    ::
			FN_TRACE_GETINFO(DEFAULT)
	WHERE   traceid = 1
			AND property = 2;

	-- rip apart file name into pieces
	SET @filename = REVERSE(@filename);
	SET @bc = CHARINDEX('.', @filename);
	SET @ec = CHARINDEX('_', @filename) + 1;
	SET @efn = REVERSE(SUBSTRING(@filename, 1, @bc));
	SET @bfn = REVERSE(SUBSTRING(@filename, @ec, LEN(@filename)));

	-- set filename without rollover number
	SET @filename = @bfn + @efn

	-- process all trace files
	SELECT  ftg.StartTime ,
			te.name AS EventName ,
			RTRIM(CAST(DB_NAME(ftg.databaseid)AS VARCHAR)) AS DatabaseName ,
			RTRIM(CAST(ftg.Filename AS VARCHAR)) AS [Filename] ,
			( ftg.IntegerData * 8 ) / 1024.0 AS GrowthMB ,
			( ftg.duration / 1000 ) AS DurMS
	FROM    ::
			FN_TRACE_GETTABLE(@filename, DEFAULT) AS ftg
			INNER JOIN sys.trace_events AS te ON ftg.EventClass = te.trace_event_id
	WHERE   ( ftg.EventClass = 92		-- Date File Auto-grow
			  OR ftg.EventClass = 93	-- Log File Auto-grow
			)
			AND DB_NAME(ftg.databaseid) = @DB
	ORDER BY ftg.StartTime
	
	
END

IF @infoCPU = 'TRUE' BEGIN

	-- Recent CPU Utilization History (SQL 2008/2008 R2 and 2012 Only)
	-- Get CPU Utilization History for last 256 minutes (in one minute intervals)
	PRINT '##########  CPU Utilization #############';
	DECLARE @ts_now bigint
	SELECT @ts_now = cpu_ticks/(cpu_ticks/ms_ticks)FROM sys.dm_os_sys_info;

	SELECT TOP(256) SQLProcessUtilization AS [SQL Server Process CPU Utilization], 
				   SystemIdle AS [System Idle Process], 
				   100 - SystemIdle - SQLProcessUtilization AS [Other Process CPU Utilization], 
				   DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) AS [Event Time] 
	FROM ( 
		  SELECT record.value('(./Record/@id)[1]', 'int') AS record_id, 
				record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') 
				AS [SystemIdle], 
				record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 
				'int') 
				AS [SQLProcessUtilization], [timestamp] 
		  FROM ( 
				SELECT [timestamp], CONVERT(xml, record) AS [record] 
				FROM sys.dm_os_ring_buffers WITH (NOLOCK)
				WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
				AND record LIKE N'%<SystemHealth>%') AS x 
		  ) AS y 
	ORDER BY record_id DESC OPTION (RECOMPILE);

	-- Signal Waits for instance
	PRINT '##########  Signat Waits #############';
	SELECT CAST(100.0 * SUM(signal_wait_time_ms) / SUM (wait_time_ms) AS NUMERIC(20,2)) 
	AS [%signal (cpu) waits],
	CAST(100.0 * SUM(wait_time_ms - signal_wait_time_ms) / SUM (wait_time_ms) AS NUMERIC(20,2)) 
	AS [%resource waits]
	FROM sys.dm_os_wait_stats WITH (NOLOCK) OPTION (RECOMPILE);

	-- Signal Waits above 10-15% is usually a sign of CPU pressure
	
	-- Get CPU utilization by database 
	PRINT '##########  CPU Utilization by Database #############';
	WITH DB_CPU_Stats
	AS
		(SELECT DatabaseID, DB_Name(DatabaseID) AS [Database Name], SUM(total_worker_time) AS [CPU_Time_Ms]
	FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY (SELECT CONVERT(int, value) AS [DatabaseID] 
              FROM sys.dm_exec_plan_attributes(qs.plan_handle)
              WHERE attribute = N'dbid') AS F_DB
		GROUP BY DatabaseID)
	SELECT ROW_NUMBER() OVER(ORDER BY [CPU_Time_Ms] DESC) AS [CPU Rank],
       RTRIM(CAST ([Database Name] AS VARCHAR)) AS [Database Name], 
	   [CPU_Time_Ms] AS [CPU Time (ms)], 
       CAST([CPU_Time_Ms] * 1.0 / SUM([CPU_Time_Ms]) OVER() * 100.0 AS DECIMAL(5, 2)) AS [CPU Percent]
	FROM DB_CPU_Stats
	WHERE DatabaseID <> 32767 -- ResourceDB
		ORDER BY [CPU Rank] OPTION (RECOMPILE);
	

END

IF @infoMEM = 'TRUE' BEGIN

	-- Page Life Expectancy (PLE) value for current instance
	PRINT '##########  Page Life Expectancy #############';
	SELECT RTRIM(CAST(ServerProperty('servername') AS VARCHAR(50))) AS [Server Name], 
		RTRIM(CAST([object_name] AS VARCHAR(50))) AS [Object Name], 
		cntr_value AS [Page Life Expectancy]
	FROM sys.dm_os_performance_counters WITH (NOLOCK)
	WHERE [object_name] LIKE N'%Buffer Manager%' -- Handles named instances
	AND counter_name = N'Page life expectancy' OPTION (RECOMPILE);

	-- PLE is a good measurement of memory pressure.
	-- Higher PLE is better. Watch the trend, not the absolute value.
	
	-- Memory Grants Pending value for current instance
	-- Run several times in quick succession
	DECLARE @COUNTER INT
	DECLARE @PARAM1 VARCHAR(72)
	DECLARE @PARAM2 VARCHAR(72)
	DECLARE @PARAM3 VARCHAR(10)

	PRINT CHAR(13) + + CHAR(13)
	PRINT '#########  MEMORY GRANTS PENDING	##########'
	SET @COUNTER = 0
	DECLARE my_cursor CURSOR FOR
		SELECT CAST(ServerProperty('servername') AS VARCHAR(50)) AS [Server Name], 
			[object_name], 
			cntr_value AS [Memory Grants Pending]                                                                                                       
		FROM sys.dm_os_performance_counters WITH (NOLOCK)
		WHERE [object_name] LIKE N'%Memory Manager%' -- Handles named instances
		AND counter_name = N'Memory Grants Pending' OPTION (RECOMPILE)

	OPEN my_cursor

	FETCH NEXT FROM my_cursor
	INTO @PARAM1, @PARAM2, @PARAM3;

	WHILE @@FETCH_STATUS = 0
		WHILE @COUNTER <=15
			BEGIN
				PRINT @PARAM1 + '          ' + @PARAM2 + @PARAM3
				SET @COUNTER = @COUNTER + 1
				FETCH NEXT FROM my_cursor
				INTO @PARAM1, @PARAM2, @PARAM3
			END

	CLOSE my_cursor
	DEALLOCATE my_cursor

	-- Memory Grants Outstanding above zero for a sustained period is a very strong indicator of memory pressure
	-- Specifies the total number of processes that have successfully acquired a workspace memory grant.
	
	-- Get total buffer usage by database for current instance
	-- This make take some time to run on a busy instance
	
	PRINT CHAR(13);
	PRINT '##########  Total Buffer Usage by DB #############';
	
	SELECT RTRIM(CAST(DB_NAME(database_id) AS VARCHAR)) AS [Database Name],
		COUNT(*) * 8/1024.0 AS [Cached Size (MB)]
	FROM sys.dm_os_buffer_descriptors
	WHERE database_id > 4 -- system databases
	AND database_id <> 32767 -- ResourceDB
	GROUP BY DB_NAME(database_id)
	ORDER BY [Cached Size (MB)] DESC OPTION (RECOMPILE);

	-- Information about what specifically is in the buffer for @DB
	PRINT '##########  Objects in buffer for @DB #############';
	IF @BufferSpecific = 'TRUE' BEGIN
	
	EXEC ('USE ' + @DB + '
	SELECT COUNT(*) AS cached_pages_count, 
			RTRIM(CAST (name AS VARCHAR)) AS [BaseTableName], 
			RTRIM(CAST (IndexName AS VARCHAR(50))) AS [IndexName], 
			RTRIM(CAST (IndexTypeDesc AS VARCHAR)) AS [IndexTypeDesc]
	FROM sys.dm_os_buffer_descriptors AS bd
		INNER JOIN
		(
	SELECT s_obj.name, 
			s_obj.index_id, 
			s_obj.allocation_unit_id, 
			s_obj.OBJECT_ID,
			i.name IndexName, 
			i.type_desc IndexTypeDesc
	FROM
		(
	SELECT OBJECT_NAME(OBJECT_ID) AS name, 
			index_id, 
			allocation_unit_id, 
			OBJECT_ID
	FROM sys.allocation_units AS au
		INNER JOIN sys.partitions AS p
		ON au.container_id = p.hobt_id
		AND (au.type = 1 OR au.type = 3)
		UNION ALL
	SELECT OBJECT_NAME(OBJECT_ID) AS name,
		index_id, 
		allocation_unit_id, 
		OBJECT_ID
	FROM sys.allocation_units AS au
		INNER JOIN sys.partitions AS p
		ON au.container_id = p.partition_id
		AND au.type = 2
		) AS s_obj
		LEFT JOIN sys.indexes i ON i.index_id = s_obj.index_id
		AND i.OBJECT_ID = s_obj.OBJECT_ID ) AS obj
		ON bd.allocation_unit_id = obj.allocation_unit_id
	WHERE database_id = DB_ID()
		GROUP BY name, index_id, IndexName, IndexTypeDesc
		ORDER BY cached_pages_count DESC;
	')
	
	END
	
	-- Good basic information about operating system memory amounts and state
	PRINT '##########  OS Memory #############';
	SELECT total_physical_memory_kb, available_physical_memory_kb, 
		   total_page_file_kb, available_page_file_kb, 
		   substring(system_memory_state_desc,1,75) [system_memory_state_desc]
	FROM sys.dm_os_sys_memory WITH (NOLOCK) OPTION (RECOMPILE);

	-- You want to see "Available physical memory is high"
	-- This indicates that you are not under external memory pressure

	-- SQL Server Process Address space info 
	--(shows whether locked pages is enabled, among other things)
	PRINT '##########  Process Address Space Info #############';
	SELECT physical_memory_in_use_kb,locked_page_allocations_kb, 
		   page_fault_count, memory_utilization_percentage, 
		   available_commit_limit_kb, process_physical_memory_low, 
		   process_virtual_memory_low
	FROM sys.dm_os_process_memory WITH (NOLOCK) OPTION (RECOMPILE);

	-- You want to see 0 for process_physical_memory_low
	-- You want to see 0 for process_virtual_memory_low
	-- This indicates that you are not under internal memory pressure

	-- Tells you how much memory (in the buffer pool) is being used by each database on the instance
	PRINT '##########  Instance Memory Settings #############';
	SELECT  RTRIM(CAST(c.minimum AS VARCHAR)) AS [Min Memory (MB)], 
			RTRIM(CAST(c.value_in_use AS VARCHAR)) AS [Max Memory (MB)] ,
			( CAST(m.total_physical_memory_kb AS BIGINT) / 1024 ) [Server Memory (MB)]
	FROM    sys.dm_os_sys_memory m
			INNER JOIN sys.configurations c ON c.name = 'max server memory (MB)'
	WHERE   CAST(m.total_physical_memory_kb AS BIGINT) < ( CAST(c.value_in_use AS BIGINT) * 1024 )

END

IF @infoJOBS = 'TRUE' BEGIN

	-- Get SQL Server Agent jobs and Category information 
	PRINT '##########  SQL Agent Jobs #############';
	SELECT RTRIM(CAST(sj.name AS VARCHAR(50))) AS [JobName],
		SUBSTRING(sj.[description], 1, 87) AS [JobDescription],
		RTRIM(CAST(SUSER_SNAME(sj.owner_sid) AS VARCHAR)) AS [JobOwner],
		sj.date_created, 
		sj.[enabled], 
		sj.notify_email_operator_id, 
		RTRIM(CAST(sc.name AS VARCHAR)) AS [CategoryName]
	FROM msdb.dbo.sysjobs AS sj WITH (NOLOCK)
		INNER JOIN msdb.dbo.syscategories AS sc WITH (NOLOCK)
		ON sj.category_id = sc.category_id
		ORDER BY sj.name OPTION (RECOMPILE);

END


IF @infoSEC = 'TRUE' BEGIN
	-- Who has sysadmin privileges	
	PRINT '##########  Who has Sysadmin Privileges #############';
	SELECT  'Sysadmins'[Sysadmins],
			( 'Login: [' + RTRIM(CAST(l.name as varchar(50))) + ']') AS [Details]
	FROM    master.sys.syslogins l
	WHERE   l.sysadmin = 1
			AND l.name <> SUSER_SNAME(0x01)
			AND l.denylogin = 0;

	-- Who has security admin privileges 
	-- With security admin, this user can give other users (INCLUDING THEMSELVES) permissions to do everything	
	PRINT '##########  Who has security admin privileges #############';
	SELECT  RTRIM(CAST('Security Admin' AS VARCHAR)) AS [SECAdmin], 
			RTRIM(CAST(l.name AS VARCHAR(50))) AS [Login]   
    FROM    master.sys.syslogins l
    WHERE   l.securityadmin = 1
			AND l.name <> SUSER_SNAME(0x01)
            AND l.denylogin = 0 ;

END

IF @infoFRAG = 'TRUE' or @Inventory = 'TRUE' BEGIN
    -- Check Fragmentation on all indexes in @DB 
	PRINT '##########  Index Fragmentation #############';
	EXEC ('USE ' + @DB +'
	
	SELECT RTRIM(CAST(t.name AS VARCHAR)) AS TableName, 
		RTRIM(CAST(b.name AS VARCHAR)) AS IndexName, 
		i.rowcnt, 
		ps.avg_fragmentation_in_percent
	FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, NULL) AS ps
		INNER JOIN sys.indexes AS b ON ps.OBJECT_ID = b.OBJECT_ID
		INNER JOIN sys.tables AS t on ps.OBJECT_ID = t.OBJECT_ID
		INNER JOIN sysindexes i on b.name = i.name
		AND ps.index_id = b.index_id
	WHERE ps.database_id = DB_ID()
		and b.name is not null
	ORDER BY t.name, b.name
	')

END

IF @infoSTAT = 'TRUE' or @Inventory = 'TRUE' BEGIN
   -- Check when statistics were last gathered on indexes
   PRINT '##########  Last Statistice Gather Date #############';
	EXEC ('USE ' + @DB + '
	SELECT RTRIM(CAST(t.name AS VARCHAR)) AS TABLE_NAME, 
		RTRIM(CAST(i.name AS VARCHAR)) AS INDEX_NAME, 
		STATS_DATE(i.OBJECT_ID, INDEX_ID) AS StatsUpdated
	FROM SYS.INDEXES i
	INNER JOIN SYS.TABLES t on t.OBJECT_ID = i.OBJECT_ID
	ORDER BY t.name
	')

END
