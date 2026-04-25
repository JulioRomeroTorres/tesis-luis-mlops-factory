################# FUENTES USADAS ######################
-- 1. `{{project_id}}.genai_documents.auna_documents`
-- 2. `{{project_id}}.genai_documents.auna_epicrisis_mvp`

DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

##############################################################
########### PASO 01: Creación Tabla EPICRISIS OCR  ###########
##############################################################

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_sample`
AS
WITH source_file as (
  SELECT *,
    invoice_number as num_factura_documento_ocr,
    --REPLACE(REGEXP_EXTRACT(file_path, r'F\d+_\d+'), "_", "-") as  num_factura_documento_ocr,
    REGEXP_EXTRACT(file_name, r'^(\d{11})') AS ruc_emisor_path,
  FROM `{{project_id}}.genai_documents.auna_documents`
  -- ESTO SERÁ TEMPORAL
  --WHERE processed_date = '2025-07-09'
  WHERE processed_date = PERIODO_INI--'2025-07-09' AND '2025-07-19'
  QUALIFY
      ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY processed_date DESC,cutt_off_date DESC) = 1
-- where created_at > '2025-06-25 00:00:0.00 UTC'
)
select
  ep.*,
  num_factura_documento_ocr,
  ruc_emisor_path,
from  source_file  -- solo hay 5 documentos para la fecha 14 corte 16
inner join `{{project_id}}.genai_documents.auna_epicrisis_mvp` as ep 
on  source_file.id = ep.documento_id
;




#################################################################
####### PASO 02: Limpiar Fecha Ingreso y Egreso #################
#################################################################
-- Quiero limpiar info_ingreso. fec_ingreso y que esté en formato DATETIME (algo así 2023-12-13T08:36:00)

CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_clean` AS
SELECT 
  *,
  
  -- Limpieza y conversión de fec_ingreso
  CASE 
    WHEN REGEXP_CONTAINS(TRIM(info_ingreso.fec_ingreso), r'^\d{2}/\d{2}/\d{4}$')
         AND TRIM(info_ingreso.fec_ingreso) != '00/00/0000'
    THEN PARSE_DATETIME('%d/%m/%Y', TRIM(info_ingreso.fec_ingreso))
    ELSE NULL
  END AS fec_ingreso_epi_ocr,

  -- Limpieza y conversión de fec_egreso
  CASE 
    WHEN REGEXP_CONTAINS(TRIM(info_egreso.fec_egreso), r'^\d{2}/\d{2}/\d{4}$')
         AND TRIM(info_egreso.fec_egreso) != '00/00/0000'
    THEN PARSE_DATETIME('%d/%m/%Y', TRIM(info_egreso.fec_egreso))
    ELSE NULL
  END AS fec_egreso_epi_ocr

FROM `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_sample`
;




----- VALIDACION FECHA info_ingreso. fec_ingreso
-- SELECT DISTINCT info_ingreso. fec_ingreso FROM `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_sample`
-- SELECT DISTINCT fec_ingreso_epi_clean FROM `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_clean`
-- SELECT DISTINCT fec_egreso_epi_clean FROM `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_clean`


#################################################################
################ PASO 03: Limpiar DX Entrada ####################
#################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_clean` AS

SELECT
  t.*,
  (
    SELECT det_norm
    FROM (
      SELECT
        -- Normalización extendida del detalle
        UPPER(
          REGEXP_REPLACE(
            TRANSLATE(
              REGEXP_REPLACE(
                TRIM(TRANSLATE(d.detalle, 'ÁÉÍÓÚÜÑáéíóúüñ', 'AEIOUUNAEIOUUN')),
                r'\s+', ' '
              ),
              'ΑΒΕΖΗΙΚΜΝΟΡΤΥΧ',  -- Letras griegas visualmente similares
              'ABEZHIKMNOPTYX'   -- Equivalentes latinos
            ),
            r'\s+', ' '
          )
        ) AS det_norm,
        COUNT(*) AS n,
        MIN(off) AS first_pos
      FROM UNNEST(t.info_ingreso.diagnosticos_ingreso) AS d WITH OFFSET off
      WHERE d.detalle IS NOT NULL
        AND TRIM(d.detalle) <> ''
        AND d.cie10 IS NOT NULL
        AND TRIM(d.cie10) <> ''
        AND UPPER(TRIM(d.cie10)) <> 'STRING'
      GROUP BY det_norm
      ORDER BY n DESC, first_pos ASC, det_norm
      LIMIT 1
    )
  ) AS dx_ingreso_epi_ocr -- select num_factura_documento_ocr, *
FROM `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_clean` t
;





---------- VALIDACION DX ENTRADA
-- SELECT DISTINCT d.detalle
-- FROM `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_clean` t,
-- UNNEST(t.info_ingreso.diagnosticos_ingreso) AS d
-- WHERE d.detalle IS NOT NULL
-- ORDER BY 1
-- ;



###################################################################################################
################# PASO 04: Quedarnos con facturas únicas en Epicrisis OCR #########################
################################################################################################### 

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_sample_unico` AS
WITH tabla_ini AS (
  SELECT *
  FROM `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_clean`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY num_factura_documento_ocr
    ORDER BY LENGTH(dx_ingreso_epi_ocr) DESC
  ) = 1
)
SELECT * FROM tabla_ini
;






###################################################################################################
########### PASO 05: Crear tabla de campos necesarios del OCR de la hoja Epicrisis ################
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_epicrisis`
AS
SELECT
  num_factura_documento_ocr as num_factura_epicrisis_ocr,
  ruc_emisor_path as ruc_epi_ocr,
  dx_ingreso_epi_ocr,
  fec_ingreso_epi_ocr,
  fec_egreso_epi_ocr,
  info_egreso. estadia as dias_hosp_epi_ocr -- select *
FROM `{{project_id}}.siniestro_salud_auna.auna_epicrisis_mvp_sample_unico`
WHERE TRUE
AND encabezado. titulo = 'Epicrisis'
AND info_egreso. estadia IS NOT NULL
;



------------ VALIDACION FACTURAS DUPLICADAS (debería salir tabla vacía)
WITH facturas_duplicadas AS (
  SELECT 
    num_factura_epicrisis_ocr,
    COUNT(*) as conteo
  --FROM `{{project_id}}.siniestro_salud_auna.auna_consolidated_settlement_mvp_sample_unico`
  FROM `{{project_id}}.siniestro_salud_auna.ocr_data_epicrisis`
  GROUP BY num_factura_epicrisis_ocr
  HAVING COUNT(*) > 1
  --ORDER BY 2 DESC
)
SELECT t.*
--FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` t
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_epicrisis` t
JOIN facturas_duplicadas fd ON t.num_factura_epicrisis_ocr = fd.num_factura_epicrisis_ocr
ORDER BY t.num_factura_epicrisis_ocr -- puedes ordenar por otros campos relevantes
;

