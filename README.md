# 🧹 DeleteOldLogs – Procedimiento de Limpieza Dinámica de Logs

Este repositorio contiene el procedimiento almacenado `DeleteOldLogs`, diseñado para realizar la **eliminación por lotes de registros antiguos** en tablas de log, de forma dinámica, configurable y segura.

Está orientado a mantener el tamaño de las bases de datos bajo control, especialmente en entornos no productivos como *desarollo*, donde los datos de log pueden crecer rápidamente.

---

## 📌 Objetivo

Permitir la **limpieza automatizada** de tablas de logs según una política de retención basada en fecha, minimizando el uso del log de transacciones y gestionando respaldos automáticos si es necesario.

---

## ⚙️ Parámetros

| Parámetro               | Tipo             | Descripción |
|-------------------------|------------------|-------------|
| `@BatchSize`            | `INT`            | Cantidad de registros a eliminar por lote. *(Default: 100000)* |
| `@TimeStamp`            | `INT`            | Número de días de retención. Solo se conservan registros más recientes. *(Obligatorio)* |
| `@LogTreshold`          | `DECIMAL(5,2)`   | Porcentaje máximo de uso del log antes de activar backup. *(Default: 70.00)* |
| `@TableNames`           | `VARCHAR(MAX)`   | Nombres de las tablas a limpiar, separadas por coma. *(Obligatorio)* |
| `@DatabaseNames`        | `VARCHAR(MAX)`   | Nombres de las bases de datos a incluir. *(Opcional – si se omite, el procedimiento busca las bases de datos necesarias)* |
| `@SchemaNames`          | `VARCHAR(MAX)`   | Esquemas a considerar. *(Default: 'Ninguno')* |
| `@ColumnTableNames`     | `VARCHAR(MAX)`   | Columnas clave de cada tabla, en formato `[IdColumn, DateColumn]`. *(Obligatorio)* |
| `@DelayBetweenBatches`  | `INT`            | Segundos de espera entre lotes de eliminación. *(Default: 5)* |
| `@MaxRetries`           | `INT`            | Número máximo de reintentos ante error. *(Default: 5)* |
| `@EnableAutoCleanLog`   | `BIT`            | Habilita la limpieza automática del log de transacciones. *(Default: 1)* |
| `@BackupPath`           | `VARCHAR(MAX)`   | Ruta donde se almacenarán los backups del log si se activa la limpieza automática. |

---
## Consideraciones
🕒 Se recomienda ejecutar este procedimiento fuera del horario de alto tráfico.

💾 Realiza respaldos de tus bases de datos antes de utilizarlo en ambientes productivos.

📌 Cada tabla que se desea limpiar debe cumplir con:

Tener una columna de identificador único.

Tener una columna de tipo fecha/hora que se utilizará como criterio de eliminación.


---

## 💡 Ejemplo de uso

```sql
DECLARE
    @BatchSize INT = 100000,
    @TimeStamp INT = 3,
    @LogTreshold DECIMAL(5,2) = 70.00,
    @TableNames VARCHAR(MAX) = 'Table1, Table2...',
    @DatabaseNames VARCHAR(MAX) = NULL,
    @SchemaNames VARCHAR(MAX) = 'dbo, Maintenance...',
    @ColumnTableNames VARCHAR(MAX) = '[idColumn1, DateColumn1], [idColumn2, DateColumn2]...',
    @DelayBetweenBatches INT = 5,
    @MaxRetries INT = 5,
    @EnableAutoCleanLog BIT = 1,
    @BackupPath VARCHAR(MAX) = 'C:\Temp\';

EXEC master.dbo.DeleteOldLogs 
    @BatchSize,
    @TimeStamp,
    @LogTreshold,
    @TableNames,
    @DatabaseNames,
    @SchemaNames,
    @ColumnTableNames,
    @DelayBetweenBatches,
    @MaxRetries,
    @EnableAutoCleanLog,
    @BackupPath;

