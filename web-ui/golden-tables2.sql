-- Databricks notebook source
-- =============================================================================
-- DDL: Tablas Serving - Dashboard de Observabilidad de Agentes IA
-- Capa: Gold (Serving Layer)
-- Periodicidad: Real-time (Microbatch 10 seconds - 08:00 hrs a 20:00 hrs)
-- Propietario: Equipo IA / Observabilidad
-- Fuente: Raw Delta Tables en ADLS
-- Consumidor: Backend DuckDB → Dashboard Frontend
-- Versión: 1.1 | Abril 2026
-- Cambios v1.1:
--   - Eliminado grain week y month en todas las tablas (innecesario para 1 día)
--   - Eliminado cntageents de art_kpis_consolidated (codagent es dimensión, siempre sería 1)
--   - Profundidad histórica acotada a 1 día por diseño del pipeline
-- =============================================================================

-- COMMAND ----------

CREATE WIDGET TEXT p_catalog        DEFAULT 'hive_metastore';
CREATE WIDGET TEXT p_schema         DEFAULT 'aiasdata';
CREATE WIDGET TEXT p_container_name DEFAULT 'aiasdash';
CREATE WIDGET TEXT p_storage_name   DEFAULT 'stacaiaseu2d02';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${p_catalog}.${p_schema}
COMMENT 'Capa gold: tablas serving pre-agregadas para el dashboard de observabilidad de agentes IA. Consumidas por backend DuckDB vía ADLS. Actualización near real-time via microbatch cada 10 segundos.';

-- COMMAND ----------

-- =============================================================================
-- 1. art_kpis_consolidated
--    Grains activos: minute | hour | day
--    Propósito: KPIs consolidados para las cards superiores del dashboard.
--               Una fila por combinación de dimensiones + grain + tsbucket.
--    Gráficos:  Cards: Conversaciones, Interacciones,
--               Total Input Tokens, Total Output Tokens, Latencia Promedio.
-- =============================================================================

DROP TABLE IF EXISTS ${p_catalog}.${p_schema}.art_kpis_consolidated;
CREATE TABLE IF NOT EXISTS ${p_catalog}.${p_schema}.art_kpis_consolidated (

  -- Claves de dimensión (filtros del dashboard)
  nmowner           STRING        COMMENT 'Propietario del agente. Filtro principal del panel superior.',
  nmsquad           STRING        COMMENT 'Equipo o squad responsable del agente.',
  codagent          STRING        COMMENT 'Identificador único del agente.',
  nmagent           STRING        COMMENT 'Nombre descriptivo del agente.',
  codidentity       STRING        COMMENT 'Identidad asociada: usuario (email) o proceso (nombre técnico).',
  coddomain         STRING        COMMENT 'Dominio principal de la identidad (ej: corporate.com).',
  codsubdomain      STRING        COMMENT 'Subdominio o área funcional (ej: Ingeniería, Ventas).',

  -- Dimensión temporal
  descgrain         STRING        COMMENT 'Granularidad temporal del bucket: minute | hour | day.',
  tsbucket          TIMESTAMP     COMMENT 'Marca de tiempo de inicio del período representado por esta fila.',
  dtbucket          DATE          COMMENT 'Fecha del bucket. Usada como columna de partición.',

  -- Métricas
  cntconversations  INT           COMMENT 'Cantidad de conversaciones únicas registradas en el período.',
  cntinteractions   BIGINT        COMMENT 'Cantidad total de interacciones (trazas) en el período.',
  qtyintoken        BIGINT        COMMENT 'Suma total de tokens de entrada consumidos.',
  qtyouttoken       BIGINT        COMMENT 'Suma total de tokens de salida generados.',
  amntlatencyavg    DECIMAL(18,2) COMMENT 'Latencia promedio en milisegundos para el período.'

)
USING DELTA
COMMENT 'KPIs consolidados por agente, identidad y granularidad temporal para cards superiores del dashboard. Grains: minute | hour | day. Profundidad: 1 día. Sin cntageents: codagent es dimensión.'
PARTITIONED BY (descgrain, dtbucket)
LOCATION 'abfss://${p_container_name}@${p_storage_name}.dfs.core.windows.net/data/out/art_kpis_consolidated'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- COMMAND ----------

-- =============================================================================
-- 2. art_interactions_summary
--    Grains: no aplica grain. Granularidad fija: fecha x hora x día_semana.
--    Propósito: Interacciones por hora y día de semana.
--               Alimenta heatmap y cards de Top Dominio/Subdominio/Identidad.
-- =============================================================================

DROP TABLE IF EXISTS ${p_catalog}.${p_schema}.art_interactions_summary;
CREATE TABLE IF NOT EXISTS ${p_catalog}.${p_schema}.art_interactions_summary (

  -- Claves de dimensión
  nmowner           STRING        COMMENT 'Propietario del agente.',
  nmsquad           STRING        COMMENT 'Equipo o squad responsable del agente.',
  codagent          STRING        COMMENT 'Identificador único del agente.',
  codidentity       STRING        COMMENT 'Identidad asociada: usuario (email) o proceso.',
  descidentitytype  STRING        COMMENT 'Tipo de identidad: PERSON | PROCESS.',
  coddomain         STRING        COMMENT 'Dominio principal de la identidad.',
  codsubdomain      STRING        COMMENT 'Subdominio o área funcional.',

  -- Dimensión temporal
  dtdate            DATE          COMMENT 'Fecha del registro. Usada para filtrar rangos en frontend y como partición.',
  descdayofweek     STRING        COMMENT 'Día de la semana: Mon | Tue | Wed | Thu | Fri | Sat | Sun.',
  idxhour           INT           COMMENT 'Hora del día de 0 a 23. Eje del heatmap.',

  -- Métricas
  cntinteractions   BIGINT        COMMENT 'Cantidad de interacciones en la combinación dimensión + fecha + hora.'

)
USING DELTA
COMMENT 'Interacciones por hora y día de semana. Sin columna grain: granularidad fija hora x día. Alimenta heatmap, cards Top Dominio/Subdominio/Identidad y tabla de identidades. Profundidad: 1 día.'
PARTITIONED BY (dtdate)
LOCATION 'abfss://${p_container_name}@${p_storage_name}.dfs.core.windows.net/data/out/art_interactions_summary'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- COMMAND ----------

-- =============================================================================
-- 3. art_token_usage_cost
--    Grains activos: minute | hour | day
--    Propósito: Consumo de tokens y costos por modelo y agente.
-- =============================================================================

DROP TABLE IF EXISTS ${p_catalog}.${p_schema}.art_token_usage_cost;
CREATE TABLE IF NOT EXISTS ${p_catalog}.${p_schema}.art_token_usage_cost (

  -- Claves de dimensión
  nmowner           STRING        COMMENT 'Propietario del agente.',
  nmsquad           STRING        COMMENT 'Equipo o squad responsable del agente.',
  codagent          STRING        COMMENT 'Identificador único del agente.',
  nmagent           STRING        COMMENT 'Nombre descriptivo del agente.',
  codidentity       STRING        COMMENT 'Identidad asociada: usuario (email) o proceso.',
  nmmodel           STRING        COMMENT 'Nombre del modelo LLM invocado (ej: gpt-4o, claude-3.5, llama-3).',

  -- Dimensión temporal
  descgrain         STRING        COMMENT 'Granularidad temporal: minute | hour | day.',
  tsbucket          TIMESTAMP     COMMENT 'Marca de tiempo de inicio del período del bucket.',
  dtbucket          DATE          COMMENT 'Fecha del bucket. Usada como columna de partición.',

  -- Métricas de tokens
  qtyintoken        BIGINT        COMMENT 'Tokens de entrada consumidos en el período.',
  qtyouttoken       BIGINT        COMMENT 'Tokens de salida generados en el período.',

  -- Métricas de costo
  amntcostin        DECIMAL(18,6) COMMENT 'Costo en USD por tokens de entrada. 6 decimales para micro-costos.',
  amntcostout       DECIMAL(18,6) COMMENT 'Costo en USD por tokens de salida. 6 decimales para micro-costos.'

)
USING DELTA
COMMENT 'Consumo de tokens y costos por agente, modelo y granularidad. Grains: minute | hour | day. Alimenta Token Consumption Over Time, Usage by Agent, Model Usage, Cost charts.'
PARTITIONED BY (descgrain, dtbucket)
LOCATION 'abfss://${p_container_name}@${p_storage_name}.dfs.core.windows.net/data/out/art_token_usage_cost'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- COMMAND ----------

-- =============================================================================
-- 4. art_agent_metrics
--    Grains activos: minute | hour | day
--    Propósito: Métricas de rendimiento agregadas por agente.
--               Incluye percentiles de latencia, tasa de éxito y costo total.
-- =============================================================================

DROP TABLE IF EXISTS ${p_catalog}.${p_schema}.art_agent_metrics;
CREATE TABLE IF NOT EXISTS ${p_catalog}.${p_schema}.art_agent_metrics (

  -- Claves de dimensión
  nmowner           STRING        COMMENT 'Propietario del agente.',
  nmsquad           STRING        COMMENT 'Equipo o squad responsable del agente.',
  codagent          STRING        COMMENT 'Identificador único del agente.',
  nmagent           STRING        COMMENT 'Nombre descriptivo del agente.',
  codidentity       STRING        COMMENT 'Identidad asociada: usuario (email) o proceso.',
  nmmodel           STRING        COMMENT 'Nombre del modelo LLM principal invocado por el agente.',

  -- Dimensión temporal
  descgrain         STRING        COMMENT 'Granularidad temporal: minute | hour | day.',
  tsbucket          TIMESTAMP     COMMENT 'Marca de tiempo de inicio del período del bucket.',
  dtbucket          DATE          COMMENT 'Fecha del bucket. Usada como columna de partición.',

  -- Métricas de volumen
  cntops            INT           COMMENT 'Cantidad total de operaciones del agente en el período.',

  -- Métricas de latencia
  amntdurationavg   DECIMAL(18,2) COMMENT 'Duración promedio de ejecución en milisegundos.',
  amntdurationp50   DECIMAL(18,2) COMMENT 'Percentil 50 (mediana) de duración en milisegundos.',
  amntdurationp95   DECIMAL(18,2) COMMENT 'Percentil 95 de duración en milisegundos.',
  amntdurationp99   DECIMAL(18,2) COMMENT 'Percentil 99 de duración en milisegundos.',

  -- Métricas de calidad
  pctsuccessrate    DECIMAL(5,4)  COMMENT 'Tasa de éxito. Decimal: 0.9640 = 96.40%.',

  -- Métricas de costo
  amntcosttotal     DECIMAL(18,6) COMMENT 'Costo total en USD acumulado por el agente en el período.'

)
USING DELTA
COMMENT 'Métricas de rendimiento por agente y granularidad. Grains: minute | hour | day. Alimenta lollipop chart Agent Performance Summary y gauge Success Rate.'
PARTITIONED BY (descgrain, dtbucket)
LOCATION 'abfss://${p_container_name}@${p_storage_name}.dfs.core.windows.net/data/out/art_agent_metrics'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- COMMAND ----------

-- =============================================================================
-- 6. art_tool_invocations
--    Grains activos: minute | hour | day
--    Propósito: Salud y estadísticas de herramientas invocadas por agentes.
-- =============================================================================

DROP TABLE IF EXISTS ${p_catalog}.${p_schema}.art_tool_invocations;
CREATE TABLE IF NOT EXISTS ${p_catalog}.${p_schema}.art_tool_invocations (

  -- Claves de dimensión
  nmowner           STRING        COMMENT 'Propietario del agente que invoca la herramienta.',
  nmsquad           STRING        COMMENT 'Equipo o squad responsable.',
  codagent          STRING        COMMENT 'Identificador del agente que invoca la herramienta.',
  codidentity       STRING        COMMENT 'Identidad asociada que generó la invocación.',
  nmtool            STRING        COMMENT 'Nombre de la herramienta invocada (ej: database_query, image_gen).',

  -- Dimensión temporal
  descgrain         STRING        COMMENT 'Granularidad temporal: minute | hour | day.',
  tsbucket          TIMESTAMP     COMMENT 'Marca de tiempo de inicio del período del bucket.',
  dtbucket          DATE          COMMENT 'Fecha del bucket. Usada como columna de partición.',

  -- Métricas
  cntinvocations    INT           COMMENT 'Cantidad total de invocaciones de la herramienta en el período.',
  amntdurationavg   DECIMAL(18,2) COMMENT 'Duración promedio de ejecución de la herramienta en ms.',
  pcterrorrate      DECIMAL(5,4)  COMMENT 'Tasa de error. Decimal: 0.25 = 25%.'

)
USING DELTA
COMMENT 'Salud y estadísticas de herramientas invocadas por agentes. Grains: minute | hour | day. Alimenta tabla Tools Usage Invocation del dashboard.'
PARTITIONED BY (descgrain, dtbucket)
LOCATION 'abfss://${p_container_name}@${p_storage_name}.dfs.core.windows.net/data/out/art_tool_invocations'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- COMMAND ----------

-- =============================================================================
-- 7. art_traces_details
--    Sin grain. Granularidad individual de traza.
--    Propósito: Últimas N trazas individuales para la tabla Traces Detail.
--               Pipeline acota a últimas 24-48h (~5000 registros).
-- =============================================================================

DROP TABLE IF EXISTS ${p_catalog}.${p_schema}.art_traces_details;
CREATE TABLE IF NOT EXISTS ${p_catalog}.${p_schema}.art_traces_details (

  -- Identificadores
  idtrace           STRING        COMMENT 'Identificador único de la traza (UUID).',

  -- Dimensiones del agente
  codagent          STRING        COMMENT 'Identificador del agente que ejecutó la traza.',
  nmagent           STRING        COMMENT 'Nombre descriptivo del agente.',
  nmowner           STRING        COMMENT 'Propietario del agente.',
  nmsquad           STRING        COMMENT 'Equipo o squad responsable.',

  -- Dimensiones de identidad y dominio
  codidentity       STRING        COMMENT 'Identidad que generó la traza: usuario (email) o proceso.',
  coddomain         STRING        COMMENT 'Dominio donde se ejecutó la traza.',
  codsubdomain      STRING        COMMENT 'Subdominio donde se ejecutó la traza.',

  -- Detalle de ejecución
  flgsuccess        BOOLEAN       COMMENT 'Resultado: true = SUCCESS | false = FAILED.',
  amntdurationms    INT           COMMENT 'Duración total de la traza en milisegundos.',

  -- Tokens y costos
  qtyintoken        INT           COMMENT 'Tokens de entrada consumidos en esta traza.',
  qtyouttoken       INT           COMMENT 'Tokens de salida generados en esta traza.',
  amntcostin        DECIMAL(18,6) COMMENT 'Costo en USD por tokens de entrada de esta traza.',
  amntcostout       DECIMAL(18,6) COMMENT 'Costo en USD por tokens de salida de esta traza.',

  -- Timestamp
  tsstart           TIMESTAMP     COMMENT 'Timestamp de inicio de la traza.',
  dsstart           DATE          COMMENT 'Fecha de inicio. Usada como columna de partición.'

)
USING DELTA
COMMENT 'Trazas individuales de agentes. Sin grain: es la única tabla no agregada. Acotada a últimas 24-48h por el pipeline (~5000 registros). Alimenta tabla Traces Detail del dashboard.'
PARTITIONED BY (dsstart)
LOCATION 'abfss://${p_container_name}@${p_storage_name}.dfs.core.windows.net/data/out/art_traces_details'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
