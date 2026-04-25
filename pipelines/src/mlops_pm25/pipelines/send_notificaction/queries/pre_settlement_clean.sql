################## 01_FAB_SS_PreLiquidacion_Limpieza ###########################

# Nombre Query: 01_FAB_SS_PreLiquidacion_Limpieza
# Objetivo: Seleccionar los campos correctos y limpios de la hoja siteds del OCR
# Objetivos: 
# - O1: Limpiar las fechas (calidad de datos)
# - O2: Delimitar los casos según fecha de emisión solicitada
# - O2: Obtener la factura única (fecha más reciente)

######################################################################

####################################
######### Fuentes Usadas ###########
####################################
-- 1. `{{project_id}}.genai_documents.auna_documents`
-- 2. `{{project_id}}.genai_documents.auna_pre_settlement_mvp`
-- 

######################################

DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

######################################################################
############ PASO 01: Creación Tabla Preliquidacion OCR  #############
######################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample`
AS
WITH source_file as (
  SELECT *,
    invoice_number as num_factura_documento_ocr,
    --REPLACE(REGEXP_EXTRACT(file_path, r'F\d+_\d+'), "_", "-") as  num_factura_documento_ocr,
    REGEXP_EXTRACT(file_name, r'^(\d{11})') AS ruc_emisor_path,
  FROM `{{project_id}}.genai_documents.auna_documents`
  -- ESTO SERÁ TEMPORAL
  WHERE processed_date = PERIODO_INI --'2025-07-09' AND '2025-07-19' 
  QUALIFY
      ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY processed_date ASC,cutt_off_date ASC) = 1
-- where created_at > '2025-06-25 00:00:0.00 UTC'
)
select
  preli.*,
  num_factura_documento_ocr,
  ruc_emisor_path
from  source_file 
inner join `{{project_id}}.genai_documents.auna_pre_settlement_mvp` as preli
on  source_file.id = preli.documento_id
;





######################################################################
############## PASO 02.1: Fecha de Ingreso - Limpieza  ###############
######################################################################
-- Limpiar cabecera.fec_ingreso
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample_clean` AS
SELECT
  *,
  
  -- ✅ Extrae '02/06/2025 08:24' y convierte a DATETIME
  SAFE.PARSE_DATETIME(
    '%d/%m/%Y %H:%M',
    REGEXP_EXTRACT(cabecera.fec_ingreso, r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2})')
  ) AS fec_ingreso_clean,

  -- Diagnóstico
  CASE
    WHEN SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M', REGEXP_EXTRACT(cabecera.fec_ingreso, r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2})')) IS NULL 
    THEN 'Formato inválido'
    ELSE 'Válido'
  END AS estado_fecha,

  cabecera.fec_ingreso AS fec_ingreso_original

FROM `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample`
;


-- VALIDACION DE FECHA INGRESO
-- SELECT DISTINCT cabecera.fec_ingreso FROM `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample`
-- SELECT DISTINCT fec_ingreso_clean FROM `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample_clean`


######################################################################
############## PASO 02.2: Fecha de Alta - Limpieza  ##################
######################################################################

-- Limpiar cabecera.fec_alta
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample_clean_v1` AS
SELECT
  *,

  -- ✅ Extrae y convierte a DATETIME con hora incluida
  SAFE.PARSE_DATETIME(
    '%d/%m/%Y %H:%M',
    REGEXP_EXTRACT(cabecera.fec_alta, r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2})')
  ) AS fec_alta_clean,

  -- 🧪 Diagnóstico del parseo
  CASE
    WHEN SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M', REGEXP_EXTRACT(cabecera.fec_alta, r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2})')) IS NULL 
      THEN 'Formato inválido'
    ELSE 'Válido'
  END AS estado_fecha_alta,

  -- 🗓️ Campo original
  cabecera.fec_alta AS fec_alta_original

FROM
  `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample_clean`
;

-- VALIDACION DE FECHA INGRESO
-- SELECT DISTINCT cabecera.fec_alta FROM `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample_clean`
-- SELECT DISTINCT fec_alta_clean FROM `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample_clean`




###################################################################################################
################## PASO 03: Quedarnos con facturas únicas en PreLiqui OCR #########################
################################################################################################### 
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample_unico`
AS WITH
tabla_ini AS (
select
*

/*from `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_Katherin`*/
FROM `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample_clean_v1`
 
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY num_factura_documento_ocr ORDER BY
      gastos_afectos.total_facturar IS NULL,gastos_afectos.total_facturar asc) = 1
)

FROM tabla_ini
;


######################################################################
############## PASO 04: Tabla PREFinal PreLiquidacion  ###############
######################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion_preliminar` AS
SELECT
  a.* EXCEPT(num_factura_documento_ocr, fec_ingreso_clean, fec_alta_clean),
  a.num_factura_documento_ocr AS num_factura_preliqui_ocr,
  a.gastos_afectos.total_facturar AS monto_total_preliqui_ocr,
  a.fec_ingreso_clean AS fec_ingreso_preliqui_ocr, 
  a.fec_alta_clean AS fec_alta_preliqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_pre_settlement_mvp_sample_unico` a
;

--- SELECT * FROM `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion` WHERE num_factura_preliqui_ocr = 'F711-00008876'


######################################################################
############## PASO 04: Tabla FINAL Preliquidacion  ##################
######################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion` AS
SELECT
  num_factura_preliqui_ocr,
  monto_total_preliqui_ocr,
  fec_ingreso_preliqui_ocr,
  fec_alta_preliqui_ocr,
  ruc_emisor_path as ruc_preliqui_ocr,
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion_preliminar`
;



###################################################################################################
############## PASO 05 (OPCIONAL): Analisis de nulos en Preliquidacion OCR ########################
###################################################################################################


SELECT 
  'num_factura_preliqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(num_factura_preliqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion`

UNION ALL SELECT 
  'monto_total_preliqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(monto_total_preliqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion`

UNION ALL SELECT 
  'fec_ingreso_preliqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(fec_ingreso_preliqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion`

UNION ALL SELECT 
  'fec_alta_preliqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(fec_alta_preliqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion`

UNION ALL SELECT 
  'ruc_preliqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(ruc_preliqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion`

ORDER BY cantidad_nulos DESC
;


