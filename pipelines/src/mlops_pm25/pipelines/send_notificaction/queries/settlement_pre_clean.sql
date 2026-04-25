################## 01_FAB_SS_Liquidacion_Limpieza ###########################

# Nombre Query: 01_FAB_SS_Liquidacion_Limpieza
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
-- 2. `{{project_id}}.genai_documents.auna_consolidated_settlemet` 
-- 
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
############# PASO 01: Creación Tabla Liquidacion OCR  ###############
######################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample`
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
  --processed_date,
  liqui.*,
  num_factura_documento_ocr,
  ruc_emisor_path
from  source_file 
inner join `{{project_id}}.genai_documents.auna_consolidated_settlemet` as liqui
on  source_file.id = liqui.documento_id
;





######################################################################
############## PASO 02.1: Fecha de Ingreso - Limpieza  ###############
######################################################################
-- Limpiar cabecera.fec_ingreso
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` AS
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

FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample`
;


-- VALIDACION DE FECHA INGRESO
-- SELECT DISTINCT cabecera.fec_ingreso FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample`
-- SELECT DISTINCT fec_ingreso_clean FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean`


######################################################################
############## PASO 02.2: Fecha de Alta - Limpieza  ##################
######################################################################

-- Limpiar cabecera.fec_alta
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v1` AS
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
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean`
;

-- VALIDACION DE FECHA INGRESO
-- SELECT DISTINCT cabecera.fec_alta FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean`
-- SELECT DISTINCT fec_alta_clean FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean`
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` WHERE num_factura_documento_ocr = 'F136-00017565'


######################################################################
################ PASO 02.3: Monto Final - Limpieza  ##################
######################################################################
-- Quiero limpiar gastos_afectos.total_facturar
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` WHERE num_factura_documento_ocr = 'F135-00022241'
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v2` AS
SELECT
  *,
  CASE 
    WHEN gastos_afectos.total_facturar IS NULL THEN calculo_cpm. total_facturar ELSE gastos_afectos.total_facturar 
  END AS monto_total_liqui_ocr,
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v1`
;

-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` WHERE monto_total_liqui_ocr = 0
-- SELECT DISTINCT * FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` 
-- WHERE monto_total_liqui_ocr IS NULL

-- SELECT DISTINCT num_factura_documento_ocr, cabecera.mecanismo, gastos_afectos.total_facturar, calculo_cpm. total_facturar, monto_total_liqui_ocr FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` WHERE monto_total_liqui_ocr IS NULL




######################################################################
################ PASO 02.4: DNI Paciente - Limpieza  #################
######################################################################
-- Quiero limpiar cabecera.num_documento
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v3` AS
SELECT
  *,
  -- Limpieza de cabecera.num_documento
  CASE
    WHEN TRIM(cabecera.num_documento) = '' THEN NULL
    WHEN cabecera.num_documento in ('null') THEN NULL
    ELSE cabecera.num_documento
  END AS num_documento_liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v2`
;


-- SELECT DISTINCT cabecera. num_documento FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean`
--SELECT DISTINCT num_factura_documento_ocr, num_documento_liqui_ocr FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` WHERE num_documento_limpio = '10409314304'





######################################################################
################ PASO 02.5: Deducible - Limpieza  ####################
######################################################################
-- SELECT DISTINCT cabecera.deducible FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean`
-- Quiero limpiar cabecera.deducible y nombrarlo como deducible. La idea sería que en caso no haya ningún número dejarlo como null. El valor debe ser el número (ejemplo: 'S/. 20 (con IGV)'' sería 20).
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v4` AS
SELECT
  *,
  -- Limpieza de deducible
  CASE
    WHEN cabecera.deducible IS NULL THEN NULL
    WHEN REGEXP_EXTRACT(cabecera.deducible, r'\d+(?:\.\d+)?') IS NULL THEN NULL
    ELSE SAFE_CAST(REGEXP_EXTRACT(cabecera.deducible, r'\d+(?:\.\d+)?') AS FLOAT64)
  END AS deducible
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v3`
;




######################################################################
################ PASO 02.6: Mecanismo - Limpieza  ####################
######################################################################
-- SELECT cabecera.mecanismo, COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` GROUP BY ALL
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v5` AS
SELECT
  *,
  -- Limpieza de mecanismo
  CASE
    WHEN cabecera.mecanismo IS NULL OR cabecera.mecanismo in ('null', '', ' ') THEN NULL
    WHEN UPPER(cabecera.mecanismo) = 'PAGO POR SERVICIOS' THEN 'PPS'
    WHEN UPPER(cabecera.mecanismo) IN ('CPM', 'AMBULATORIO CPM') THEN 'CPM'
    WHEN UPPER(cabecera.mecanismo) IN ('PAQUETE QUIRURGICO') THEN 'PQ'
    WHEN UPPER(cabecera.mecanismo) IN ('PACIENTE MES CRONICO') THEN 'PMC' -- AUNA NO USA ESTO
    ELSE NULL
  END AS mecanismo_clean
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v4`
;




######################################################################
############# PASO 02.7: Cod Autorizacion - Limpieza  ################
######################################################################
-- SELECT cabecera.num_autoriz, COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` GROUP BY ALL
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v6` AS
SELECT
  *,
  -- Limpieza de num_autoriz
  CASE
    WHEN TRIM(cabecera.num_autoriz) = '' THEN NULL
    WHEN NOT REGEXP_CONTAINS(cabecera.num_autoriz, r'\d') THEN NULL
    ELSE cabecera.num_autoriz
  END AS cod_autorizacion_liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v5`
;

------ VALIDACION
-- SELECT cod_autorizacion_liqui_ocr, COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` GROUP BY ALL




######################################################################
############# PASO 02.8: Coaseguro - Limpieza  ################
######################################################################
-- SELECT cabecera.coaseguro, COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` GROUP BY ALL
-- Quiero limpiar el campo cabecera.coaseguro (es un string) y nombrarlo como pct_coaseguro_liqui_ocr. Eliminar espacios en blanco y símbolo ('%'). La idea sería que en caso no haya ningún número sea null, en caso contenga un punto o coma sea null o si no es múltiplo de 5 sea null, pero el valor 0 o 0.0 sí es válido. El valor resultante debe ser un número entero (ejemplo: 1626.97, sería null; 45% sería 45; 0 % sería 0)
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v7` AS
SELECT
  *,
  -- Limpieza de cabecera.coaseguro
  CASE
    WHEN TRIM(REPLACE(cabecera.coaseguro, '%', '')) = '' THEN NULL
    WHEN NOT REGEXP_CONTAINS(cabecera.coaseguro, r'\d') THEN NULL
    WHEN REGEXP_CONTAINS(cabecera.coaseguro, r'[.,]') THEN NULL
    ELSE
      -- Convertimos a INT y validamos múltiplo de 5 o si es 0
      CASE
        WHEN SAFE_CAST(TRIM(REPLACE(cabecera.coaseguro, '%', '')) AS INT64) IS NULL THEN NULL
        WHEN SAFE_CAST(TRIM(REPLACE(cabecera.coaseguro, '%', '')) AS INT64) = 0 THEN 0
        WHEN MOD(SAFE_CAST(TRIM(REPLACE(cabecera.coaseguro, '%', '')) AS INT64), 5) = 0 THEN SAFE_CAST(TRIM(REPLACE(cabecera.coaseguro, '%', '')) AS INT64)
        ELSE NULL
      END
  END AS pct_coaseguro_liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v6`
;


###################################################################################################
###################### PASO 02.9: Beneficio(cobertura) - Limpieza #################################
###################################################################################################
-- CREATE OR REPLACE TABLE
--   {{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v8 
-- AS
-- SELECT
--   *,
--   CASE 
--     WHEN REGEXP_CONTAINS(UPPER(cabecera.beneficio), r'CONSULTA\s*AMB') 
--       OR REGEXP_CONTAINS(UPPER(cabecera.beneficio), r'CONSULTA\s*MED') THEN 1
--     ELSE 0
--   END AS flag_consultaAmboMed_liqui_ocr
-- FROM {{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v7
-- ;

------ VALIDACION pct_coaseguro_liqui_ocr
-- SELECT pct_coaseguro_liqui_ocr, COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean` GROUP BY ALL


###################################################################################################
###################### PASO 02.9: Beneficio(descripcion) - Limpieza ###############################
###################################################################################################

-- Quiero crear un flag flag_consultaAmboMed_liqui_ocr, que busque por todo subgrupos.items.descripcion (es un array) el valor 'CONSULTA MED%' O 'CONSULTA AMB%' y que el flag_consultaAmboMed_liqui_ocr sea 1 en caso se encuentre uno de esos valores.
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v8` 
AS
WITH base AS (
  SELECT
    *,
    CASE 
      WHEN EXISTS (
        SELECT 1
        FROM UNNEST(subgrupos) AS subgrupo,
             UNNEST(subgrupo.items) AS item
        WHERE UPPER(item.descripcion) LIKE 'CONSULTA MED%'
           OR UPPER(item.descripcion) LIKE 'CONSULTA AMB%'
      ) THEN 1
      ELSE 0
    END AS flag_consultaAmboMed_liqui_ocr_raw
  FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v7`
),
flags_por_factura AS (
  SELECT
    num_factura_documento_ocr,
    MAX(flag_consultaAmboMed_liqui_ocr_raw) AS flag_consultaAmboMed_liqui_ocr
  FROM base
  GROUP BY num_factura_documento_ocr
)

SELECT
  base.*,
  flags_por_factura.flag_consultaAmboMed_liqui_ocr
FROM base
LEFT JOIN flags_por_factura
USING (num_factura_documento_ocr)
;




###################################################################################################
######################## PASO 02.10: Cabecera.titulo - Limpieza ###################################
###################################################################################################
-- SELECT DISTINCT cabecera.titulo FROM `rs-nprd-dlk-ia-dev-aif-d3d9.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v8` 
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v9` 
AS
SELECT
  *,
  CASE 
    WHEN cabecera.titulo IN ('null', 'nul', 'NULL', 'NUL') OR cabecera.titulo IS NULL THEN NULL
    ELSE cabecera.titulo
  END AS cabecera_titulo_liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v8`
;



###################################################################################################
########################### PASO 02.11: Subtotal - Limpieza #######################################
###################################################################################################
-- Quiero limpiar gastos_afectos.subtotal_3 y que se llame gastos_afectos_subtotal3_liqui_ocr. La idea es agarrar el MAX de ese campo. Es decir, un num_factura_documento_ocr puede tener varios registros, la idea es que este nuevo campo se le asigne el máximo valor de gastos_afectos.subtotal_3.
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v10` 
AS
WITH max_subtotal AS (
  SELECT
    num_factura_documento_ocr,
    MAX(gastos_afectos.subtotal_3) AS gastos_afectos_subtotal3_liqui_ocr
  FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v9`
  GROUP BY num_factura_documento_ocr
)

SELECT
  base.*,
  max_subtotal.gastos_afectos_subtotal3_liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v9` AS base
LEFT JOIN max_subtotal
ON base.num_factura_documento_ocr = max_subtotal.num_factura_documento_ocr
;





###################################################################################################
################## PASO 03: Quedarnos con facturas únicas en Liqui OCR ############################
###################################################################################################

-- 1. Evaluar si el campo mecanismo_clean es CPM o PPS
-- 2. PPS: Quedarnos con aquellas donde cabecera.titulo no sea NULL
-- 3. PPS: Agarrar el mayor monto
-- 4. CPM: A

-- 1. Quedarnos netamente con aquellas donde cabecera.titulo no sea NULL
-- 2. Si netamente hay un registro donde es null, quedarnos con ese, solo si hay solo un registro 
-- 3. Si hay más de una, agarrar la del mayor monto
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_unico`
AS
WITH tabla_base AS (
  SELECT
    *,
    COUNT(*) OVER (PARTITION BY num_factura_documento_ocr) AS total_por_factura,
    COUNTIF(cabecera_titulo_liqui_ocr IS NOT NULL) OVER (PARTITION BY num_factura_documento_ocr) AS con_titulo
  FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v10`
),
tabla_rank AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY num_factura_documento_ocr
      ORDER BY 
        CASE 
          WHEN cabecera_titulo_liqui_ocr IS NOT NULL THEN 1
          WHEN cabecera_titulo_liqui_ocr IS NULL AND total_por_factura = 1 THEN 2
          ELSE 3
        END,
        monto_total_liqui_ocr DESC
    ) AS rn
  FROM tabla_base
)
SELECT *
FROM tabla_rank
WHERE rn = 1
;

--- SELECT DISTINCT gastos_afectos. subtotal_2, gastos_afectos.coaseguro_igv, * FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_unico` WHERE num_factura_documento_ocr = 'F136-00017896'


-- ------------ VALIDACION FACTURAS DUPLICADAS (debería salir tabla vacía)
-- WITH facturas_duplicadas AS (
--   SELECT 
--     num_factura_documento_ocr,
--     COUNT(*) as conteo
--   --FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_unico`
--   FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_unico`
--   GROUP BY num_factura_documento_ocr
--   HAVING COUNT(*) > 1
--   --ORDER BY 2 DESC
-- )
-- SELECT t.*
-- --FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` t
-- FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_unico` t
-- JOIN facturas_duplicadas fd ON t.num_factura_documento_ocr = fd.num_factura_documento_ocr
-- ORDER BY t.num_factura_documento_ocr -- puedes ordenar por otros campos relevantes
-- ;




######################################################################
############## PASO 04: Tabla PreFINAL Liquidacion  ##################
######################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion_preliminar` AS
SELECT
  a.* EXCEPT(num_factura_documento_ocr, fec_ingreso_clean, fec_alta_clean, deducible, gastos_afectos_subtotal3_liqui_ocr),
  a.num_factura_documento_ocr AS num_factura_liqui_ocr,
  a.fec_ingreso_clean AS fec_ingreso_liqui_ocr, 
  a.fec_alta_clean AS fec_alta_liqui_ocr,
  a.gastos_afectos.subtotal_2 AS gastos_afectos_subtotal2_liqui_ocr, 
  a.gastos_afectos_subtotal3_liqui_ocr, 
  a.gastos_afectos.coaseguro_igv AS gastos_afectos_coaseguroPaciente_liqui_ocr,
  deducible as deducible_liqui_ocr,
  mecanismo_clean as mecanismo_liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_unico` a
;

-- SELECT DISTINCT mecanismo_liqui_ocr FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_unico` WHERE num_factura_documento_ocr = 'F136-00017007'

######################################################################
############## PASO 05: Tabla FINAL Liquidacion  #####################
######################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion` AS
SELECT
  num_factura_liqui_ocr,
  monto_total_liqui_ocr,
  fec_ingreso_liqui_ocr,
  fec_alta_liqui_ocr,
  gastos_afectos_subtotal2_liqui_ocr,
  gastos_afectos_subtotal3_liqui_ocr,
  gastos_afectos_coaseguroPaciente_liqui_ocr,
  pct_coaseguro_liqui_ocr,
  deducible_liqui_ocr,
  mecanismo_liqui_ocr,
  flag_consultaAmboMed_liqui_ocr,
  cod_autorizacion_liqui_ocr,
  num_documento_liqui_ocr,
  ruc_emisor_path as ruc_liqui_ocr,
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion_preliminar`
;

--- SELECT * FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion` WHERE num_factura_liqui_ocr = 'F160-00007555'
--'F136-00017007'


###################################################################################################
##################### PASO 06: Quedarnos con facturas de CPM en Liqui OCR #########################
###################################################################################################

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_cpm`
AS
SELECT 
  * 
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_clean_v10`
WHERE mecanismo_clean = 'CPM' 
;



######################################################################
############# PASO 07: Tabla PreFINAL Liquidacion CPM ################
######################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion_preliminar_cpm` AS
SELECT
  a.* EXCEPT(num_factura_documento_ocr, fec_ingreso_clean, fec_alta_clean, deducible, gastos_afectos_subtotal3_liqui_ocr),
  a.num_factura_documento_ocr AS num_factura_liqui_ocr,
  a.fec_ingreso_clean AS fec_ingreso_liqui_ocr, 
  a.fec_alta_clean AS fec_alta_liqui_ocr,
  a.gastos_afectos.subtotal_2 AS gastos_afectos_subtotal2_liqui_ocr, 
  a.gastos_afectos_subtotal3_liqui_ocr, 
  a.gastos_afectos.coaseguro_igv AS gastos_afectos_coaseguroPaciente_liqui_ocr,
  deducible as deducible_liqui_ocr,
  mecanismo_clean as mecanismo_liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_cpm` a
;


######################################################################
############## PASO 08: Tabla FINAL Liquidacion CPM ##################
######################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion_pre_cpm` AS
SELECT
  num_factura_liqui_ocr,
  monto_total_liqui_ocr,
  fec_ingreso_liqui_ocr,
  fec_alta_liqui_ocr,
  gastos_afectos_subtotal2_liqui_ocr,
  gastos_afectos_subtotal3_liqui_ocr,
  gastos_afectos_coaseguroPaciente_liqui_ocr,
  pct_coaseguro_liqui_ocr,
  deducible_liqui_ocr,
  mecanismo_liqui_ocr,
  flag_consultaAmboMed_liqui_ocr,
  cod_autorizacion_liqui_ocr,
  num_documento_liqui_ocr,
  ruc_emisor_path as ruc_liqui_ocr,
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion_preliminar_cpm`
;


######################################################################
############## PASO 09: Tabla FINAL Liquidacion CPM ##################
######################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion_cpm` AS
SELECT
  num_factura_liqui_ocr,
  ruc_liqui_ocr,
  cod_autorizacion_liqui_ocr,
  num_documento_liqui_ocr,
  MAX(monto_total_liqui_ocr) AS monto_total_liqui_ocr,
  MAX(fec_ingreso_liqui_ocr) AS fec_ingreso_liqui_ocr,
  MAX(fec_alta_liqui_ocr) AS fec_alta_liqui_ocr,
  MAX(gastos_afectos_subtotal2_liqui_ocr) AS gastos_afectos_subtotal2_liqui_ocr,
  MAX(gastos_afectos_subtotal3_liqui_ocr) AS gastos_afectos_subtotal3_liqui_ocr,
  MAX(gastos_afectos_coaseguroPaciente_liqui_ocr) AS gastos_afectos_coaseguroPaciente_liqui_ocr,
  MAX(pct_coaseguro_liqui_ocr) AS pct_coaseguro_liqui_ocr,
  MAX(deducible_liqui_ocr) AS deducible_liqui_ocr,
  mecanismo_liqui_ocr,
  MAX(flag_consultaAmboMed_liqui_ocr) AS flag_consultaAmboMed_liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion_pre_cpm`
GROUP BY ALL
;



###################################################################################################
############## PASO 06 (OPCIONAL): Analisis de nulos en Liquidacion OCR ###########################
###################################################################################################

SELECT 
  'num_factura_liqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(num_factura_liqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`

UNION ALL SELECT 
  'monto_total_liqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(monto_total_liqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`

UNION ALL SELECT 
  'fec_ingreso_liqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(fec_ingreso_liqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`

UNION ALL SELECT 
  'gastos_afectos_subtotal2_liqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(gastos_afectos_subtotal2_liqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`

UNION ALL SELECT 
  'gastos_afectos_coaseguroPaciente_liqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(gastos_afectos_coaseguroPaciente_liqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`

UNION ALL SELECT 
  'fec_alta_liqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(fec_alta_liqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`

UNION ALL SELECT 
  'num_documento_liqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(num_documento_liqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`


UNION ALL SELECT 
  'ruc_liqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(ruc_liqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`

UNION ALL SELECT 
  'pct_coaseguro_liqui_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(pct_coaseguro_liqui_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`

ORDER BY cantidad_nulos DESC
;


