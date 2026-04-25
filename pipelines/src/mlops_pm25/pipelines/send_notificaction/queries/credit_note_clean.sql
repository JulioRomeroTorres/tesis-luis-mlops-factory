################## 01_FAB_SS_NotaCredito_Limpieza ###########################

# Nombre Query: 01_FAB_SS_NotaCredito_Limpieza
# Objetivo: Seleccionar los campos correctos y limpios de la hoja Nota de Credito del OCR. Además, usar la trama para agarrar si tiene o no NC
# Objetivos: 
# - O1: Limpiar las fechas (calidad de datos)
# - O2: Delimitar los casos según fecha de emisión solicitada
# - O2: Obtener la factura única (fecha más reciente)

######################################################################

####################################
######### Fuentes Usadas ###########
####################################
-- 1. `{{project_id}}.genai_documents.auna_documents`
-- 2. `{{project_id}}.genai_documents.auna_credit_note_mvp`
-- 3. `{{project_id}}.siniestro_salud_auna.trama_notacredito_previa` -- Esto sale del query de la trama nota de credito

######################################


#######################################################################
################ PASO 01: Creación Tabla Nota Credito OCR  ############
#######################################################################

DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample`
AS
WITH source_file as (
  SELECT *,
    invoice_number as num_factura_documento_ocr
    --REPLACE(REGEXP_EXTRACT(file_path, r'F\d+_\d+'), "_", "-") as  num_factura_documento_ocr,
  FROM `{{project_id}}.genai_documents.auna_documents`
  -- ESTO SERÁ TEMPORAL
  WHERE processed_date = PERIODO_INI --'2025-07-09' AND '2025-07-19' 
-- where created_at > '2025-06-25 00:00:0.00 UTC'
)
select
  cd.*,
  num_factura_documento_ocr 
from  source_file 
inner join `{{project_id}}.genai_documents.auna_credit_note_mvp` as cd
on  source_file.id = cd.documento_id
;





##############################################################
################ PASO 02: Limpiar RUC ########################
##############################################################
-- SELECT DISTINCT recuadro.ruc FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample`
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean` AS

SELECT
  *,
  /* extraemos solo dígitos */
  CASE
    WHEN LENGTH(REGEXP_REPLACE(IFNULL(recuadro.ruc, ''), r'[^0-9]', '')) = 11
      THEN REGEXP_REPLACE(recuadro.ruc, r'[^0-9]', '')   -- ← exactamente 11 dígitos
    ELSE NULL                                            -- ← cualquier otro caso
  END AS ruc_clean
FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample`
;

---- Validacion RUC
-- SELECT DISTINCT ruc_clean FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean`



##############################################################
################ PASO 03: FECHA LIMPIEZA #####################
##############################################################
-- SELECT DISTINCT recuadro.fecha FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample`
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean_v1` AS

/* ───────── 1. Normalizar texto ───────── */
WITH et01 AS (
  SELECT
    *,
    -- a) NFD          b) sin tildes
    -- c) corrige JUNÍA (insensible a mayúsc/minúsc)   d) MAYÚSCULAS
    UPPER(
      REGEXP_REPLACE(                               -- c)
        REGEXP_REPLACE(                             -- b
          NORMALIZE(recuadro.fecha, NFD),           -- a
          r'\p{M}', ''
        ),
        r'(?i)JUNIA', 'JUNIO'
      )
    ) AS fecha_norm
  FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean`
),

/* ───────── 2. Quitar prefijo de día de semana y hora ───────── */
et02 AS (
  SELECT
    *,
    REGEXP_REPLACE(
      fecha_norm,
      r'^(LUNES|MARTES|MIERCOLES|MIÉRCOLES|JUEVES|VIERNES|SABADO|SÁBADO|DOMINGO),\s*',
      ''
    ) AS sin_prefijo
  FROM et01
),
et03 AS (   -- suprime “, HH:MM” o “ HH:MM(:SS)” con coma opcional
  SELECT
    *,
    REGEXP_REPLACE(
      sin_prefijo,
      r'\s*,?\s*\d{1,2}:\d{2}(:\d{2})?.*$',
      ''
    ) AS sin_hora
  FROM et02
),
et04 AS (   -- reemplaza conectores “ DE / DEL / .DE ”
  SELECT
    *,
    REGEXP_REPLACE(sin_hora, r'\s*(\.DE| DEL | DE )\s*', ' ') AS sin_conectores
  FROM et03
),
et05 AS (   -- limpieza final de espacios y signos
  SELECT
    *,
    REGEXP_REPLACE(
      TRIM(REGEXP_REPLACE(sin_conectores, r'[.:]$', '')),
      r'\s+',
      ' '
    ) AS texto_limpio
  FROM et04
),

/* ───────── 3. Parseo a DATE ───────── */
parsed AS (
  SELECT
    *,
    CASE
      /* 3-a  DD/MM/YYYY o DD/MM/YY */
      WHEN REGEXP_CONTAINS(texto_limpio, r'^\d{1,2}/\d{1,2}/\d{2,4}$') THEN
        SAFE.PARSE_DATE(
          '%d/%m/%Y',
          REGEXP_REPLACE(texto_limpio,
                         r'^(\d{1,2}/\d{1,2})/(\d{2})$', r'\1/20\2')
        )

      /* 3-b  ISO YYYY-MM-DD */
      WHEN REGEXP_CONTAINS(texto_limpio, r'^\d{4}-\d{2}-\d{2}$') THEN
        SAFE.PARSE_DATE('%Y-%m-%d', texto_limpio)

      /* 3-c  “DD MES YYYY” con mes en castellano */
      WHEN REGEXP_CONTAINS(texto_limpio, r'^\d{1,2} [A-ZÁÉÍÓÚÜÑ]+ \d{4}$') THEN (
        SELECT DATE(
          CAST(REGEXP_EXTRACT(texto_limpio, r'(\d{4})$') AS INT64),
          CASE REGEXP_EXTRACT(texto_limpio, r'[A-ZÁÉÍÓÚÜÑ]+')
            WHEN 'ENERO'       THEN 1   WHEN 'FEBRERO'    THEN 2
            WHEN 'MARZO'       THEN 3   WHEN 'ABRIL'      THEN 4
            WHEN 'MAYO'        THEN 5   WHEN 'JUNIO'      THEN 6
            WHEN 'JULIO'       THEN 7   WHEN 'AGOSTO'     THEN 8
            WHEN 'SEPTIEMBRE'  THEN 9   WHEN 'SETIEMBRE'  THEN 9
            WHEN 'OCTUBRE'     THEN 10  WHEN 'NOVIEMBRE'  THEN 11
            WHEN 'DICIEMBRE'   THEN 12
          END,
          CAST(REGEXP_EXTRACT(texto_limpio, r'^(\d{1,2})') AS INT64)
        )
      )
      ELSE NULL
    END AS fecha_nc_limpia
  FROM et05
)

/* ───────── 4. Tabla definitiva ───────── */
SELECT
  recuadro.fecha                              AS fecha_nc_original,   -- ① original

  parsed.* EXCEPT(fecha_norm, sin_prefijo, sin_hora,
                  sin_conectores, texto_limpio, fecha_nc_limpia),     -- ② resto

  /* ③ nueva fecha limpia + flag + derivadas */
  fecha_nc_limpia,

  fecha_nc_limpia IS NULL                    AS flag_fecha_nc_sin_parsear,
  EXTRACT(YEAR  FROM fecha_nc_limpia)        AS anio_nc,
  EXTRACT(MONTH FROM fecha_nc_limpia)        AS mes_nc,
  FORMAT_DATE('%B', fecha_nc_limpia)         AS mes_nombre_nc,
  FORMAT_DATE('%A', fecha_nc_limpia)         AS dia_semana_nc
FROM parsed
;



--- Validacion Fecha
-- SELECT DISTINCT fecha_nc_limpia FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean`
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean`





###########################################################################
###### PASO 04: TEMPORAL: LIMPIEZA DE MONTO SUBTOTAL NC (no hay en trama) ####
###########################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean_v2` 
AS
SELECT 
  *,
  resumen.subtotal AS subtotal_ocr_nc
  --B.montonota AS importe_total_ocr_nc
FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean_v1` AS A
;



###########################################################################
###### PASO 05: TEMPORAL: LIMPIEZA DE MONTO TOTAL NC (si hay en trama) ####
###########################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean_v3` 
AS
SELECT 
  *,
  resumen.importe_total AS importe_total_ocr_nc
  --B.montonota AS importe_total_ocr_nc
FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean_v2` AS A
LEFT JOIN (
  SELECT numero_de_documento_de_pago, montonota, ruc_tramaNC FROM `{{project_id}}.siniestro_salud_auna.trama_notacredito_previa`
  WHERE TIPONOTA = 'C'
) as B
ON (REPLACE(A.num_factura_documento_ocr, '-', '') = B.numero_de_documento_de_pago) AND A.ruc_clean = B.ruc_tramaNC
;




#############################################################################
#### PASO 06: TEMPORAL: LIMPIEZA DE NUMERO DE FACTUR DE REFERENCIA EN NC ####
#############################################################################
-- Quiero limpiar el campo info_principal.ref_nro y llamarlo num_factura_nc_ref_nro_ocr. Para la limpieza
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean_v4` 
AS
SELECT 
  *,
  CASE 
    WHEN info_principal.ref_nro IS NULL OR LOWER(info_principal.ref_nro) = 'null' THEN NULL
    ELSE CONCAT(
      SUBSTR(info_principal.ref_nro, 0, 5), 
      LPAD(SUBSTR(info_principal.ref_nro, 6), 8, '0')
    )
  END AS num_factura_nc_ref_nro_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean_v3`
;





###################################################################################################
################# PASO 07 (FINAL): Quedarnos con facturas únicas en NC OCR ########################
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_unico`
AS
WITH tabla_rank AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY num_factura_documento_ocr
      ORDER BY 
        CASE 
          WHEN num_factura_documento_ocr = num_factura_nc_ref_nro_ocr THEN 1
          WHEN num_factura_nc_ref_nro_ocr IS NULL THEN 2
          ELSE 3
        END
    ) AS rn,
    COUNT(*) OVER (PARTITION BY num_factura_documento_ocr) AS total_por_factura
  FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_clean_v4`
)
SELECT *
FROM tabla_rank
WHERE rn = 1
  AND (
    num_factura_documento_ocr = num_factura_nc_ref_nro_ocr
    OR num_factura_nc_ref_nro_ocr IS NULL
  )
;

-- ------------ VALIDACION FACTURAS DUPLICADAS (debería salir tabla vacía)
-- WITH facturas_duplicadas AS (
--   SELECT 
--     num_factura_documento_ocr,
--     COUNT(*) as conteo
--   FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_unico`
--   GROUP BY num_factura_documento_ocr
--   HAVING COUNT(*) > 1
--   --ORDER BY 2 DESC
-- )
-- SELECT t.*
-- FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_unico` t
-- JOIN facturas_duplicadas fd ON t.num_factura_documento_ocr = fd.num_factura_documento_ocr
-- ORDER BY t.num_factura_documento_ocr -- puedes ordenar por otros campos relevantes
-- ;



###################################################################################################
########### PASO 08: Crear tabla de campos necesarios del OCR de la hoja Nota Credito #############
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`
AS
SELECT
  *
  EXCEPT (num_factura_documento_ocr),
  num_factura_documento_ocr as num_factura_notacredito_ocr -- select *
FROM `{{project_id}}.siniestro_salud_auna.auna_credit_note_mvp_sample_unico`
;





###################################################################################################
####################### PASO 09 (OPCIONAL): Analisis de nulos en NC OCR ###########################
###################################################################################################
SELECT 
  'fecha_nc_original' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(fecha_nc_original) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'documento_id' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(documento_id) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'page_path' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(page_path) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'recuadro' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(recuadro) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'resumen' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(resumen) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'created_at' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(created_at) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'ruc_clean' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(ruc_clean) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'fecha_nc_limpia' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(fecha_nc_limpia) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'flag_fecha_nc_sin_parsear' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(flag_fecha_nc_sin_parsear) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'anio_nc' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(anio_nc) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'mes_nc' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(mes_nc) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'mes_nombre_nc' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(mes_nombre_nc) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'dia_semana_nc' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(dia_semana_nc) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

-- UNION ALL SELECT 
--   'nro_clean' AS campo,
--   COUNT(*) AS cantidad_total,
--   COUNT(*) - COUNT(nro_clean) AS cantidad_nulos
-- FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'numero_de_documento_de_pago' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(numero_de_documento_de_pago) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'montonota' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(montonota) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'ruc_tramaNC' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(ruc_tramaNC) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'importe_total_ocr_nc' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(importe_total_ocr_nc) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

UNION ALL SELECT 
  'num_factura_notacredito_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(num_factura_notacredito_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`

ORDER BY cantidad_nulos DESC;
