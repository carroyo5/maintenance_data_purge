--DECLARE
--		@BatchSize INT = 100000, --Tamaño de los lotes que se borraran por iteracion (por defecto)
--		@TimeStamp INT = 3, --Cantidad de tiempo que se quiere dejar vigente en los logs (obligatorio)
--		@LogTreshold DECIMAL (5,2) = 70.00, --Porcentaje maximo de uso del log antes de hacer un backup (por defecto)
--		@TableNames VARCHAR(MAX) = 'BackendLog, AuditLog, TechLog, ApplicationLogs', --Nombre de la(s) tabla(s) que se quieren limpiar (obligatorio)
--		@DatabaseNames VARCHAR(MAX), --Listado de Bases de Datos que se quieren incluir (opcional)
--		@SchemaNames VARCHAR(MAX) = 'dbo, EntLib', --Listado de esquemas que se quieren incluir (opcional)
--		@ColumnTableNames VARCHAR(MAX) = '[BackendLogId, LogDate], [AuditLogId, DateTime], [LogId, Timestamp], [ApplicationLogId, ExecutionDate]', --Nombre de las columnas que se usaran. Las columnas deben ser ID y TimeStamp (obligatorio)
--		@DelayBetweenBatches INT = 5, --Segundos
--		@MaxRetries INT = 5,
--		Variables de configuración para los backups
--		@EnableAutoCleanLog BIT = 1, --Activar la limpieza de logs automatica (auto por defecto)
--		@BackupPath VARCHAR(MAX) = 'D:\Temp\'

USE master
GO

CREATE OR ALTER PROCEDURE dbo.DeleteOldLogs(
		@BatchSize INT = 100000, --Tamaño de los lotes que se borraran por iteracion (por defecto)
		@TimeStamp INT, --Cantidad de tiempo que se quiere dejar vigente en los logs (obligatorio)
		@LogTreshold DECIMAL (5,2) = 70.00, --Porcentaje maximo de uso del log antes de hacer un backup (por defecto)
		@TableNames VARCHAR(MAX), --Nombre de la(s) tabla(s) que se quieren limpiar (obligatorio)
		@DatabaseNames VARCHAR(MAX), --Listado de Bases de Datos que se quieren incluir (opcional)
		@SchemaNames VARCHAR(MAX), --Listado de esquemas que se quieren incluir (opcional)
		@ColumnTableNames VARCHAR(MAX), --Nombre de las columnas que se usaran. Las columnas deben ser ID y TimeStamp (obligatorio)
		@DelayBetweenBatches INT = 5, --Segundos (default 5)
		@MaxRetries INT = 5,
		--Variables de configuración para los backups
		@EnableAutoCleanLog BIT = 1, --Activar la limpieza de logs automatica
		@BackupPath VARCHAR(MAX)
)

/*************************************
Procedimiento : dbo.DeleteOldLogs
Autor : Cristhian Arroyo
Fecha : 02-jun-2025
*************************************/


AS BEGIN

	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	
	--Variables locales
	DECLARE @StartId INT, 
			@EndId INT, 
			@LastId INT, 
			@Count INT, 
			@BatchEndId INT,
			@LogUsedPercent FLOAT,
			@LogFileName AS VARCHAR(500),
			@SQL NVARCHAR(MAX),
			@ChildSQL NVARCHAR(MAX),
			@ErrorMessage VARCHAR(MAX),
			@HAisEnabled BIT = 0,
			@MaxDelayBetweenBatches INT  = 25,
			@ETAHAwaitTime NCHAR(15) = TIMEFROMPARTS(0,0,30,0,0), --30 Segundos de espera para la disminución del tiempo de espera en replicas
			@BatchesDelayTime NCHAR(15),
			@CurrentDateTime DATE = GETDATE(),
			@Lower VARCHAR(30),
			@Upper VARCHAR(30),
			@RefObjId VARCHAR(150),
			@RefColId VARCHAR(50),
			@ChildFlag BIT,
			--Verificacion de replicas
			@MaxReplicaETA INT = 60,  --60 segundos de umbral máximo
			@send_kb FLOAT,
			@send_kb_s FLOAT,
			@redo_kb FLOAT,
			@redo_kb_s FLOAT,
			@est_seg_sent FLOAT,
			@est_seg_redo FLOAT,
			@est_replica_global FLOAT,
			@RetryCount INT,
			--Validacion de cadenas
			@Start INT = 1,
			@End INT,
			@Pair VARCHAR(MAX),
			@CommaCount INT,
			@SpaceCount INT,
			--Validación de informacion en las tablas
			@MissingTables VARCHAR(MAX),
			@MissingColumns VARCHAR(MAX),
			--Variables para identificar la Base de datos
			@DatabaseName VARCHAR(255),
			@SchemaName VARCHAR(255),
			@TableName VARCHAR(128),
			@IdColumn VARCHAR(128),
			@TimeStampColumn VARCHAR(128),
			@Exists BIT,
			--Variables de cursor (proceso de eliminacion)
			@SelectedDatabase VARCHAR(255),
			@SelectedTable VARCHAR(255),
			@SelectedIdColumn VARCHAR(255),
			@SelectedSchema VARCHAR(255),
			@SelectedTimeStampColumn VARCHAR(255);


	DECLARE @TablesInfo AS TABLE (
			Id INT IDENTITY (1,1),
			DatabaseName VARCHAR(500),
			SchemaName VARCHAR(500),
			TableName VARCHAR (500),
			IdColumn VARCHAR(500),
			TimeStampColumn VARCHAR(500),
			Information BIT,
			Processed BIT DEFAULT 0
	)

	DECLARE	@ColumnTable AS TABLE (
			Id INT IDENTITY (1,1),
			TableName VARCHAR(100),
			IdColumn VARCHAR(100),
			TimeStampColumn VARCHAR(100)
	)

	IF OBJECT_ID('tempdb..#DatabasesWSchemas') IS NULL
		BEGIN
			CREATE TABLE #DatabasesWSchemas(
					DatabaseName NVARCHAR(255),
					SchemaName NVARCHAR(255)
			)
		END
	ELSE 
		BEGIN
			TRUNCATE TABLE #DatabasesWSchemas
		END
	
	/*Construccion del parametro de espera*/
	SET @BatchesDelayTime = TIMEFROMPARTS(0,0, (CASE WHEN @DelayBetweenBatches > @MaxDelayBetweenBatches 
													THEN @MaxDelayBetweenBatches 
													ELSE @DelayBetweenBatches END),0,0);
	/*Formateo de Path*/
	SET @BackupPath = (CASE WHEN CHARINDEX('\', @BackupPath) > 0 THEN REPLACE(@BackupPath, '/', '\') ELSE REPLACE(@BackupPath, '\', '/') END)
					  +(CASE WHEN RIGHT(@BackupPath, 1) NOT IN ('/', '\') THEN (CASE WHEN CHARINDEX('/', @BackupPath) > 0 THEN '/' ELSE '\' END)
					ELSE '' END)

	/*
	----------
	Validar input de las variables
	----------
	*/

	IF (@BackupPath IS NULL AND @EnableAutoCleanLog = 1)
		BEGIN
			RAISERROR('Backup path for log backups has not been configured. Please set the backup path before continuing.', 16, 1)
			RETURN;
		END

	IF NOT EXISTS (SELECT * FROM sys.dm_os_file_exists(@BackupPath) WHERE file_is_a_directory = 1 AND parent_directory_exists = 1) AND @EnableAutoCleanLog = 1
		BEGIN
			RAISERROR('Backup path for log backups doesn''t exists or it is incorrect.', 16, 1)
			RETURN;
		END

	IF (@TimeStamp <= 0 OR @TimeStamp IS NULL)
		BEGIN
			RAISERROR('The timestamp value must be greater than 0 or cannot be NULL.', 16, 1);
			RETURN;
		END

	IF (@TableNames IS NULL OR @ColumnTableNames IS NULL)
		BEGIN
			RAISERROR('At least one table and its corresponding columns must be specified for the cleaning process.', 16, 1);
			RETURN;
		END

	IF (@DelayBetweenBatches <= 0)
		BEGIN
			RAISERROR('Delay between batches must be greater than 0.', 16, 1);
			RETURN;
		END

	IF @DelayBetweenBatches > @MaxDelayBetweenBatches
		BEGIN
			SET @ErrorMessage = 'Delay between batches has been automatically set to max established : ' + CONVERT(NCHAR(2),@MaxDelayBetweenBatches) + 's.'
			RAISERROR(@ErrorMessage, 0,1) WITH NOWAIT;
			SET @DelayBetweenBatches = @MaxDelayBetweenBatches
		END

	/*Formateo de variable*/


	/*
	----------
	Validar grupos de disponibilidad
	----------
	*/
	IF (SELECT SERVERPROPERTY('IsHadrEnabled')) = 1
		BEGIN
			SET @HAisEnabled = 1
		--verificamos si nos encontramos en el primer grupo de disponibilidad
			IF	NOT EXISTS (
				SELECT 1 FROM sys.availability_groups ag
					INNER JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
					INNER JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id
					WHERE ars.is_local = 1	--Conectado
					AND ars.role = 1	--Primaria
					AND ars.operational_state = 2 --ONLINE
					AND ars.recovery_health = 1 --ONLINE
					AND ars.synchronization_health = 2 --HEALTHY
					)
					BEGIN
						RAISERROR('Not on primary replica, please switch to primary replica or verify your replica''s health.', 16, 1);
						RETURN;
					END
		END
	ELSE
		BEGIN
			SET @HAisEnabled = 0
		END
	/*
	----------
	Validar formato de las variables
	----------
	*/
	--Nombres de las tablas
	SET @CommaCount = LEN(@TableNames) - LEN(REPLACE(@TableNames, ',', ''));
	SET @SpaceCount = LEN(@TableNames) - LEN(REPLACE(@TableNames, ' ', ''));

	IF @TableNames LIKE '%[^a-zA-Z0-9_, ]%'
	BEGIN
		RAISERROR('The table names string contains invalid characters.', 16, 1);
		RETURN;
	END;

	IF @TableNames LIKE '%,,' OR @TableNames LIKE '%, ' OR @TableNames LIKE '% ,%'
	BEGIN
		RAISERROR('The separators are not valid (must be ", ").', 16, 1);
		RETURN;
	END;

	IF @SpaceCount != @CommaCount
	BEGIN
		RAISERROR('Invalid format on table names, try the following format "Table1, Table2, Table3...".', 16, 1)
		RETURN
	END;

	IF @TableNames LIKE ' %' OR @TableNames LIKE '% '
	BEGIN
		RAISERROR('There are leading or trailing spaces in the string.', 16, 1)
		RETURN
	END;

	IF @TableNames LIKE '%  %'
	BEGIN
		RAISERROR('There are multiple spaces between table names.', 16, 1)
		RETURN
	END;

	IF LEFT(@TableNames, 1) = ',' OR RIGHT(@TableNames, 1) = ','
	BEGIN
		RAISERROR('The string cannot start or end with a comma.',16,1)
		RETURN
	END;

	IF EXISTS (SELECT value FROM STRING_SPLIT(@TableNames, ',') WHERE LTRIM(RTRIM(value)) = '')
	BEGIN
		RAISERROR('There are empty table names in the string.', 16, 1);
		RETURN;
	END;

	IF EXISTS ( SELECT LOWER(LTRIM(RTRIM(value)))
		FROM STRING_SPLIT(@TableNames, ',')
		GROUP BY LOWER(LTRIM(RTRIM(value)))
		HAVING COUNT (*) > 1)
	BEGIN
		RAISERROR('There are duplicate table names in the string.', 16, 1);
		RETURN;
	END;

	--Bases de datos
	SET @CommaCount = LEN(@DatabaseNames) - LEN(REPLACE(@DatabaseNames, ',', ''));
	SET @SpaceCount = LEN(@DatabaseNames) - LEN(REPLACE(@DatabaseNames, ' ', ''));

	IF @DatabaseNames LIKE '%[^a-zA-Z0-9_, ]%'
	BEGIN
		RAISERROR('The database names string contains invalid characters.', 16, 1);
		RETURN;
	END;

	IF @DatabaseNames LIKE '%,,' OR @DatabaseNames LIKE '%, ' OR @DatabaseNames LIKE '% ,%'
	BEGIN
		RAISERROR('The separators are not valid (must be ", ").', 16, 1);
		RETURN;
	END;

	IF @SpaceCount != @CommaCount
	BEGIN
		RAISERROR('Invalid format on database names, try the following format "Database1, Database2, Database3...".', 16, 1)
		RETURN
	END;

	IF @DatabaseNames LIKE ' %' OR @DatabaseNames LIKE '% '
	BEGIN
		RAISERROR('There are leading or trailing spaces in the string.', 16, 1)
		RETURN
	END;

	IF @DatabaseNames LIKE '%  %'
	BEGIN
		RAISERROR('There are multiple spaces between database names.', 16, 1)
		RETURN
	END;

	IF LEFT(@DatabaseNames, 1) = ',' OR RIGHT(@DatabaseNames, 1) = ','
	BEGIN
		RAISERROR('The string cannot start or end with a comma.',16,1)
		RETURN
	END;

	IF EXISTS (SELECT value FROM STRING_SPLIT(@DatabaseNames, ',') WHERE LTRIM(RTRIM(value)) = '')
	BEGIN
		RAISERROR('There are empty database names in the string.', 16, 1);
		RETURN;
	END;

	--Nombres de esquemas

	SET @CommaCount = LEN(@SchemaNames) - LEN(REPLACE(@SchemaNames, ',', ''));
	SET @SpaceCount = LEN(@SchemaNames) - LEN(REPLACE(@SchemaNames, ' ', ''));

	IF @SchemaNames LIKE '%[^a-zA-Z0-9_, ]%'
	BEGIN
		RAISERROR('The database names string contains invalid characters.', 16, 1);
		RETURN;
	END;

	IF @SchemaNames LIKE '%,,' OR @SchemaNames LIKE '%, ' OR @SchemaNames LIKE '% ,%'
	BEGIN
		RAISERROR('The separators are not valid (must be ", ").', 16, 1);
		RETURN;
	END;

	IF @SpaceCount != @CommaCount
	BEGIN
		RAISERROR('Invalid format on database names, try the following format "Database1, Database2, Database3...".', 16, 1)
		RETURN
	END;

	IF @SchemaNames LIKE ' %' OR @SchemaNames LIKE '% '
	BEGIN
		RAISERROR('There are leading or trailing spaces in the string.', 16, 1)
		RETURN
	END;

	IF @SchemaNames LIKE '%  %'
	BEGIN
		RAISERROR('There are multiple spaces between database names.', 16, 1)
		RETURN
	END;

	IF LEFT(@SchemaNames, 1) = ',' OR RIGHT(@SchemaNames, 1) = ','
	BEGIN
		RAISERROR('The string cannot start or end with a comma.',16,1)
		RETURN
	END;

	IF EXISTS (SELECT value FROM STRING_SPLIT(@SchemaNames, ',') WHERE LTRIM(RTRIM(value)) = '')
	BEGIN
		RAISERROR('There are empty database names in the string.', 16, 1);
		RETURN;
	END;

	--Nombres de columnas

	IF (@ColumnTableNames NOT LIKE '[[]%,%]' 
		OR (LEN(@ColumnTableNames) - LEN(REPLACE(@ColumnTableNames, ',', '')) % 2 = 0) 
		OR ((LEN(@ColumnTableNames) - LEN(REPLACE(@ColumnTableNames, '[', ''))) + 
		(LEN(@ColumnTableNames) - LEN(REPLACE(@ColumnTableNames, ']', '')))) % 2 <> 0)
		BEGIN
			RAISERROR('Invalid format on column names, try the following format "[ColumnId1, TimeStampColumn2],[ColumnId1, TimeStampColumn2]...".', 16, 1)
			RETURN
		END;

	IF (@ColumnTableNames LIKE '%[^a-zA-Z0-9_,[] ]%')
		BEGIN
			RAISERROR('The column names string contains invalid characters.', 16, 1)
			RETURN
		END;


	WHILE @Start <= LEN(@ColumnTableNames)
		BEGIN
			SET @End = CHARINDEX(']', @ColumnTableNames, @Start)
			SET @Start = CHARINDEX('[', @ColumnTableNames, @Start)
			IF @End = 0 BREAK

			SET @Pair = SUBSTRING(@ColumnTableNames, @Start, @End - @Start + 1)

			-- Verificar que no haya más de dos columnas por par de corchetes
			IF LEN(@Pair) - LEN(REPLACE(@Pair, ',', '')) <> 1
			BEGIN
				RAISERROR('Each bracket pair should contain exactly two columns.', 16, 1)
				RETURN
			END

			IF @Pair LIKE '%[],%' OR @Pair LIKE '%,]%'
				BEGIN
					RAISERROR('Column names cannot be empty.', 16, 1)
					RETURN
				END

			SET @Start = @End + 1
		END;

	/*
	----------
	Tratamiento de cadenas
	----------
	*/
	WITH SplittedColumnName AS (
		SELECT 
		ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Id,
			LTRIM(RTRIM(REPLACE(REPLACE(value, '[', ''), ']', ''))) AS ColumnPair 
		FROM STRING_SPLIT(REPLACE(@ColumnTableNames, '], [', '|'), '|') 
	),

	SplittedTableName AS(
	SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Id, 
	RTRIM(LTRIM(value)) AS TableName FROM STRING_SPLIT(@TableNames, ',')
	)

	INSERT INTO @ColumnTable (TableName, IdColumn, TimeStampColumn)
		SELECT TableName,
			LTRIM(RTRIM(SUBSTRING(ColumnPair, 1, CHARINDEX(',', ColumnPair) - 1))) AS Column1,
			LTRIM(RTRIM(SUBSTRING(ColumnPair, CHARINDEX(',', ColumnPair) + 1, LEN(ColumnPair)))) AS Column2
		FROM SplittedColumnName AS scn
		FULL OUTER JOIN SplittedTableName stn ON stn.Id = scn.Id;

	/*
	----------
	Buscar la Base de datos con la tabla(s)
	----------
	*/
	--Buscar bases de datos y sus esquemas
	DECLARE db_cursor CURSOR FOR
		SELECT name
		FROM sys.databases 
		WHERE state = 0
		AND (@DatabaseNames IS NULL OR name IN (SELECT RTRIM(LTRIM(value)) FROM STRING_SPLIT(@DatabaseNames, ',')));

	OPEN db_cursor;

	FETCH NEXT FROM db_cursor INTO @DatabaseName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		-- Consulta dinámica para obtener los esquemas de la base de datos actual
		SET @SQL = '
			INSERT INTO #DatabasesWSchemas (DatabaseName, SchemaName)
			SELECT CATALOG_NAME, SCHEMA_NAME
			FROM ' + QUOTENAME(@DatabaseName) + '.INFORMATION_SCHEMA.SCHEMATA;';

		EXEC sp_executesql @SQL;

		FETCH NEXT FROM db_cursor INTO @DatabaseName;
	END;

	CLOSE db_cursor;
	DEALLOCATE db_cursor;

	--Sacar los nombres de las tablas

	DECLARE table_cursor CURSOR FOR
	SELECT TableName, IdColumn, TimeStampColumn
	FROM @ColumnTable;

	OPEN table_cursor;
	FETCH NEXT FROM table_cursor INTO @TableName, @IdColumn, @TimeStampColumn;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		DECLARE db_cursor CURSOR FOR
		SELECT DatabaseName, SchemaName
		FROM #DatabasesWSchemas
		WHERE (@DatabaseNames IS NULL OR DatabaseName IN (SELECT RTRIM(LTRIM(value)) FROM STRING_SPLIT(@DatabaseNames, ',')))
		AND (@SchemaNames IS NULL OR SchemaName IN (SELECT RTRIM(LTRIM(value)) FROM STRING_SPLIT(@SchemaNames, ',')));

		OPEN db_cursor;

		FETCH NEXT FROM db_cursor INTO @DatabaseName, @SchemaName;

		WHILE @@FETCH_STATUS = 0
		BEGIN
			SET @SQL = '
				SELECT
					@Exists = CASE WHEN EXISTS (
						SELECT 1
						FROM ' + QUOTENAME(@DatabaseName) + '.INFORMATION_SCHEMA.COLUMNS
						WHERE TABLE_NAME = @TableName
						AND COLUMN_NAME = @IdColumn
						AND TABLE_SCHEMA = @TableSchema
						) AND EXISTS (SELECT 1
						FROM ' + QUOTENAME(@DatabaseName) + '.INFORMATION_SCHEMA.COLUMNS
						WHERE TABLE_NAME = @TableName
						AND COLUMN_NAME = @TimeStampColumn
						AND TABLE_SCHEMA = @TableSchema) THEN 1 ELSE 0 END;

				IF @Exists = 1
				BEGIN
					-- Validar si la tabla tiene registros
					SELECT @Exists = CASE WHEN (
						SELECT p.rows
						FROM ' + QUOTENAME(@DatabaseName) + '.sys.partitions p
						JOIN ' + QUOTENAME(@DatabaseName) + '.sys.tables t ON p.object_id = t.object_id
						JOIN ' + QUOTENAME(@DatabaseName) + '.sys.schemas s ON t.schema_id = s.schema_id
						WHERE t.name = @TableName
						AND s.name = @TableSchema
						AND p.index_id IN (0, 1)
					) > 0 THEN 1 ELSE 0 END;
				END';

			EXEC sp_executesql @SQL,
				N'@TableName VARCHAR(128), @IdColumn VARCHAR(128), @TimeStampColumn VARCHAR(128),
				  @Exists BIT OUTPUT, @TableSchema VARCHAR(255)',
				@TableName, @IdColumn, @TimeStampColumn, @Exists OUTPUT, @SchemaName;

			IF (@Exists = 1)
				BEGIN
					INSERT INTO @TablesInfo (DatabaseName, SchemaName, TableName, IdColumn, TimeStampColumn, Information) 
					VALUES (@DatabaseName, @SchemaName, @TableName, @IdColumn, @TimeStampColumn, 0);
				END;

			FETCH NEXT FROM db_cursor INTO @DatabaseName, @SchemaName;
		END;

		CLOSE db_cursor;
		DEALLOCATE db_cursor;

		FETCH NEXT FROM table_cursor INTO @TableName, @IdColumn, @TimeStampColumn;
		SET @Exists = 0; 
	END;

	/*
	----------
	Borrar datos que no seran procesados
	----------
	*/

	IF OBJECT_ID('tempdb..#DatabasesWSchemas') IS NOT NULL
		BEGIN
			DROP TABLE #DatabasesWSchemas
		END

		IF EXISTS (SELECT 1 FROM @TablesInfo ti
		RIGHT JOIN @ColumnTable ct  ON ti.TableName = ct.TableName
		WHERE ti.Id IS NULL)
		BEGIN

			SELECT @MissingTables = STRING_AGG(ct.TableName, ', '), 
				   @MissingColumns = STRING_AGG(CONCAT('[', ct.IdColumn, ', ', ct.TimeStampColumn, ']'), ', ')
			FROM @TablesInfo ti
				RIGHT JOIN @ColumnTable ct  ON ti.TableName = ct.TableName
			WHERE ti.Id IS NULL
		
			SET @ErrorMessage = 
				'The table(s) "' + 
				ISNULL(@MissingTables, 
					''
				) + 
				'" or column(s) "' + 
				ISNULL(@MissingColumns, 
					''
				) + 
				'" do not exist. Please verify the table name and column names provided in the input.';		
			RAISERROR (@ErrorMessage, 16,1) WITH NOWAIT
		END
	/*
	----------
	Proceso de eliminación de Logs
	----------
	*/

	DECLARE tables_to_clean CURSOR FOR
	SELECT DatabaseName,
		   SchemaName,
		   TableName,
		   IdColumn,
		   TimeStampColumn
		   FROM @TablesInfo

	OPEN tables_to_clean;
	FETCH NEXT FROM tables_to_clean INTO @SelectedDatabase, @SelectedSchema, @SelectedTable, @SelectedIdColumn, @SelectedTimeStampColumn

	WHILE (@@FETCH_STATUS = 0)
		BEGIN
			BEGIN TRY
				/*Construccion de la consulta dinamica para estimar el bloque que se va borrar*/
				SET @SQL = '
				SELECT @StartId = MIN(' + QUOTENAME(@SelectedIdColumn) + '),
					   @EndId = MAX(' + QUOTENAME(@SelectedIdColumn) + ')
				FROM ' + QUOTENAME(@SelectedDatabase) + '.' + QUOTENAME(@SelectedSchema) + '.' + QUOTENAME(@SelectedTable) + ' (NOLOCK)
				WHERE ' + QUOTENAME(@SelectedTimeStampColumn) + ' <= DATEADD(MONTH, -' + CAST(@TimeStamp AS NVARCHAR) + ','+QUOTENAME(CAST(@CurrentDateTime AS NVARCHAR), '''')+')';

				EXEC sp_executesql @SQL, N'@StartId INT OUTPUT, @EndId INT OUTPUT', @StartId OUTPUT, @EndId OUTPUT;
				/*Se limpia el LastId*/
				SET @LastId = NULL
				/*Se comprueba si existen hijos en las tablas padres*/
				SET @ChildFlag = 1
			END TRY
			BEGIN CATCH
				SET @ErrorMessage = 'ERROR: Failed to process table ''' + @SelectedDatabase + '.' + @SelectedSchema + '.' + @SelectedTable + '''. '
					  + 'Reason: ' + ERROR_MESSAGE() + ' '
					  + 'The process will skip this table and continue with the next one.';
				RAISERROR (@ErrorMessage, 16, 1);
			END CATCH;

			WHILE (@StartId < @EndId)
				BEGIN
				/*Verificacion de replicacion cuando se tiene HA */
					IF @HAisEnabled = 1
						BEGIN
							SET @RetryCount = 0;

							SELECT	@send_kb   = drs.log_send_queue_size,
									@send_kb_s = drs.log_send_rate,
									@redo_kb   = drs.redo_queue_size,
									@redo_kb_s = drs.redo_rate 
							    FROM sys.dm_hadr_database_replica_states AS drs
								INNER JOIN sys.databases AS d ON drs.database_id = d.database_id
								WHERE 
									d.name = @SelectedDatabase
									AND drs.is_primary_replica = 1  

							    SET @est_seg_sent = CASE WHEN @send_kb_s > 0 THEN @send_kb * 1.0 / @send_kb_s    ELSE 0 END;
								SET @est_seg_redo = CASE WHEN @redo_kb_s > 0 THEN @redo_kb * 1.0 / @redo_kb_s    ELSE 0 END;

								SET @est_replica_global = (CASE 
															 WHEN @est_seg_sent  > @est_seg_redo 
															 THEN @est_seg_sent 
															 ELSE @est_seg_redo 
														   END)

							SET @ErrorMessage = 'ETA Replica: '+ CAST(ROUND(@est_seg_sent,  2) AS VARCHAR(10))
												  + '  |  Redo=' 
												  + CAST(ROUND(@est_seg_redo,   2) AS VARCHAR(10)) 
												  + '  |  Max=' 
												  + CAST(ROUND(@est_replica_global, 2) AS VARCHAR(10)) + ' seg';

							RAISERROR(@ErrorMessage, 0,1) WITH NOWAIT;

							IF @est_replica_global > @MaxReplicaETA
								BEGIN
									SET @ErrorMessage = 'WARNING: ('+ CAST(ROUND(@est_replica_global, 2) AS VARCHAR(10)) 
														  + ' seg)ETA is over the treshold ' 
														  + CAST(@MaxReplicaETA AS VARCHAR(10)) ;
										RAISERROR(@ErrorMessage, 10,1);
								END
							
							WHILE @est_replica_global > @MaxReplicaETA AND @RetryCount <= @MaxRetries
								BEGIN
									SET @RetryCount = @RetryCount + 1;
									
									RAISERROR ('Retry count: %i', @RetryCount, 0,1);

									WAITFOR DELAY @ETAHAwaitTime
									 SELECT 
											 @send_kb   = drs.log_send_queue_size,
											 @send_kb_s = drs.log_send_rate,
											 @redo_kb   = drs.redo_queue_size,
											 @redo_kb_s = drs.redo_rate
										FROM sys.dm_hadr_database_replica_states AS drs
										INNER JOIN sys.databases             AS d
											ON drs.database_id = d.database_id
										WHERE 
											d.name             = @SelectedDatabase
											AND drs.is_primary_replica = 1;

										SET @est_seg_sent     = CASE WHEN @send_kb_s > 0 THEN @send_kb  * 1.0 / @send_kb_s  ELSE 0 END;
										SET @est_seg_redo       = CASE WHEN @redo_kb_s > 0 THEN @redo_kb  * 1.0 / @redo_kb_s  ELSE 0 END;
										SET @est_replica_global = CASE 
																	 WHEN @est_seg_sent > @est_seg_redo 
																	 THEN @est_seg_sent 
																	 ELSE @est_seg_redo 
																   END
									IF @RetryCount = @MaxRetries
										BEGIN
											RAISERROR('Retries limit reached, aborting operation. Estimation Global Replica is greater than max replica ETA (%d s).', @MaxReplicaETA, 18,1) WITH NOWAIT;
											RETURN;
										END
								END
						END

				--Control y posible backup de logs
					IF @EnableAutoCleanLog = 1
						BEGIN
							BEGIN TRY
								-- Obtener el porcentaje de uso del log y el nombre del archivo de log
								SET @SQL = '
									SELECT @LogUsedPercent = used_log_space_in_percent
									FROM ' + QUOTENAME(@SelectedDatabase) + '.sys.dm_db_log_space_usage;

									SELECT @LogFileName = name
									FROM ' + QUOTENAME(@SelectedDatabase) + '.sys.database_files
									WHERE type_desc = ''LOG'';';

								EXEC sp_executesql @SQL, 
									N'@LogUsedPercent FLOAT OUTPUT, @LogFileName VARCHAR(500) OUTPUT', 
									@LogUsedPercent OUTPUT, @LogFileName OUTPUT;

								PRINT 'Current log usage percentage: ' + CAST(@LogUsedPercent AS VARCHAR(10)) + '%';

								-- Verificar si el porcentaje de uso del log supera el umbral
								IF (@LogUsedPercent > @LogTreshold)
								BEGIN
									PRINT 'Log usage exceeds the threshold. Starting log cleanup process...';

									-- Verificar si el procedimiento almacenado DatabaseBackup existe
									IF OBJECT_ID('master.dbo.DatabaseBackup', 'P') IS NOT NULL
										BEGIN
											SET @SQL = N'
												EXECUTE [dbo].[DatabaseBackup]
													@Databases = @Databases,
													@Directory = @Directory,
													@BackupType = @BackupType,
													@Compress = @Compress,
													@Verify = @Verify,
													@CleanupTime = @CleanupTime,
													@CleanupMode = @CleanupMode,
													@CheckSum = @CheckSum,
													@LogToTable = @LogToTable;';

											EXEC sp_executesql @SQL, 
												N'@Databases NVARCHAR(50), @Directory NVARCHAR(255), @BackupType NVARCHAR(10), @Compress NVARCHAR(1), 
												@Verify NVARCHAR(1), @CleanupTime INT, @CleanupMode NVARCHAR(20), @CheckSum NVARCHAR(1), @LogToTable NVARCHAR(1)',
												@SelectedDatabase, @BackupPath, 'LOG', 'Y', 'Y', 24, 'AFTER_BACKUP', 'Y', 'Y';

											PRINT 'Log backup completed successfully.';
										END
									ELSE
										BEGIN
											SET @SQL = N'BACKUP LOG ' + QUOTENAME(@SelectedDatabase) +
														' TO DISK = ''' + @BackupPath + @SelectedDatabase + '_LOG_BACKUP_' + 
														REPLACE(REPLACE(REPLACE(CONVERT(NVARCHAR, GETDATE(), 120), ':', '_'), ' ', '_'), '-', '_') +
														'.trn'' WITH INIT, COMPRESSION, CHECKSUM;';

											EXEC sp_executesql @SQL;

											PRINT 'Log backup completed successfully using standard BACKUP LOG command.';
										END
								END
								ELSE
									BEGIN
										PRINT 'Log usage is within the acceptable threshold. No action needed.';
									END
							END TRY
							BEGIN CATCH
								-- Manejo de errores
								DECLARE @ErrorSeverity INT;
								DECLARE @ErrorState INT;

								SELECT 
									@ErrorMessage = ERROR_MESSAGE(),
									@ErrorSeverity = ERROR_SEVERITY(),
									@ErrorState = ERROR_STATE();

								PRINT 'An error occurred during the log cleanup process:';
								PRINT 'Error Message: ' + @ErrorMessage;
								PRINT 'Error Severity: ' + CAST(@ErrorSeverity AS VARCHAR(10));
								PRINT 'Error State: ' + CAST(@ErrorState AS VARCHAR(10));
								THROW;
							END CATCH
						END
					ELSE
						BEGIN
							PRINT'Current log usage percentage: '+CAST(@LogUsedPercent AS VARCHAR)+''
						END
					
					SET @BatchEndId = (CASE WHEN @StartId + @BatchSize < @EndId 
											THEN @StartId + @BatchSize 
											ELSE @EndId END)
						/*Extracción de tablas hijas cuando se intenta borrar una tabla padre*/
					PRINT @SelectedDatabase+CHAR(10)+CHAR(13)+ @SelectedSchema+CHAR(10)+CHAR(13)+@SelectedTable

					IF (@ChildFlag = 1)
						BEGIN
							SET	@Lower = CAST(ISNULL(@LastId, @StartId) AS VARCHAR(30));
							SET @Upper = CAST(@BatchEndId AS VARCHAR(30));

							SET @RefObjId = CONVERT(VARCHAR(100),
													 OBJECT_ID(
													   QUOTENAME(@SelectedDatabase) + N'.'
													 + QUOTENAME(@SelectedSchema) + N'.'
													 + QUOTENAME(@SelectedTable)
													 ));
							
							SET @SQL =  N'SELECT @RefColId = c.column_id
								 FROM ' + QUOTENAME(@SelectedDatabase) + N'.sys.columns AS c
								 WHERE c.object_id = @RefObjId
								   AND c.name      = @SelectedIdColumn;';

							EXEC sp_executesql
								 @SQL,
								 N'@RefObjId INT,
								   @SelectedIdColumn SYSNAME,
								   @RefColId INT OUTPUT',
								 @RefObjId = @RefObjId,
								 @SelectedIdColumn = @SelectedIdColumn,
								 @RefColId = @RefColId OUTPUT;

								/*Posible limitante en el script cuando se tengan llaves compuestas*/
							SET @SQL = N'
								SELECT 
									@ChildSql = STRING_AGG(
										  ''DELETE C
										FROM ' + QUOTENAME(@SelectedDatabase) + '.''
											+ QUOTENAME(s.name) + ''.'' 
											+ QUOTENAME(ch.name) + '' AS C
										WHERE C.'' + QUOTENAME(cc.name) 
												+ '' >= ' + @Lower + '
										  AND C.'' + QUOTENAME(cc.name) 
												+ '' <  ' + @Upper + ';
									'', CHAR(13) + CHAR(10))
								FROM ' + QUOTENAME(@SelectedDatabase) + '.sys.foreign_key_columns AS fkc
								JOIN ' + QUOTENAME(@SelectedDatabase) + '.sys.objects AS ch
									ON fkc.parent_object_id = ch.object_id
								JOIN ' + QUOTENAME(@SelectedDatabase) + '.sys.columns AS cc
									ON cc.object_id = ch.object_id
									AND cc.column_id = fkc.parent_column_id
								JOIN ' + QUOTENAME(@SelectedDatabase) + '.sys.schemas AS s
									ON ch.schema_id = s.schema_id
								WHERE fkc.referenced_object_id = @RefObjId
								  AND fkc.referenced_column_id = @RefColId;';

							EXEC sp_executesql
								@SQL,
								N'
								  @Lower VARCHAR(30),
								  @Upper VARCHAR(30),
								  @RefObjId	   INT,
								  @RefColId    INT,
								  @ChildSql    NVARCHAR(MAX) OUTPUT',
								@Lower		 = @Lower,
								@Upper		 = @Upper,
								@RefObjId    = @RefObjId,
								@RefColId    = @RefColId,
								@ChildSql    = @ChildSql OUTPUT;

							IF (@ChildSql <> '')
								BEGIN
									--PRINT @ChildSql
									EXEC sp_executesql @ChildSql;
									SET @ChildSql = N'';
								END
							ELSE
								BEGIN
									/*Si intenta entrar 1 vez y no encuentra tablas hijas
									entonces se configura para que no reintente más el proceso*/
									SET @ChildFlag = 0
								END
						END
					SET @SQL = 'DELETE FROM '+QUOTENAME(@SelectedDatabase)+'.'+QUOTENAME(@SelectedSchema)+'.'+QUOTENAME(@SelectedTable)+'
						WHERE '+QUOTENAME(@SelectedIdColumn)+' >= ' + CONVERT(CHAR(30), ISNULL(@LastId, @StartId))+'
						AND '+QUOTENAME(@SelectedIdColumn)+' < '+CONVERT(CHAR(30), @BatchEndId)+';'


					BEGIN TRAN
						--SELECT @SQL
						EXEC sp_executesql @SQL
					COMMIT TRAN
		
					IF (@BatchEndId >= @EndId)
						BEGIN
							SET @LastId = @EndId - @StartId
						END
					SET @StartId = @StartId + @BatchSize

					WAITFOR DELAY @BatchesDelayTime
				END
			FETCH NEXT FROM tables_to_clean INTO @SelectedDatabase, @SelectedSchema, @SelectedTable, @SelectedIdColumn, @SelectedTimeStampColumn
		END

	CLOSE tables_to_clean;
	DEALLOCATE tables_to_clean;
END