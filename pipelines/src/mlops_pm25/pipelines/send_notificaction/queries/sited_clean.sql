################## 01_FAB_SS_Siteds_Limpieza ###########################

# Nombre Query: 01_FAB_SS_Siteds_Limpieza
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
-- 2. `{{project_id}}.genai_documents.auna_sited_mvp`
-- 3. `{{project_id}}.siniestro_salud_auna.trama_sited_previa` -- Esto sale del query de trama sited
-- 4. `{{project_id}}.siniestro_salud_auna.trama_factura_rfs` -- Esto sale del query de trama sited
######################################

DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

##############################################################
################ PASO 01: Creación Tabla Siteds OCR  #########
##############################################################

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_sited_mvp_sample`
AS
WITH source_file as (
  SELECT *,
    invoice_number as num_factura_documento_ocr,
    --REPLACE(REGEXP_EXTRACT(file_path, r'F\d+_\d+'), "_", "-") as  num_factura_documento_ocr,
    REGEXP_EXTRACT(file_name, r'^(\d{11})') AS ruc_emisor_path, -- SELECT DISTINCT processed_date
  FROM `{{project_id}}.genai_documents.auna_documents` 
  -- ESTO SERÁ TEMPORAL
  WHERE processed_date = PERIODO_INI --'2025-07-09' AND '2025-07-19' 
  QUALIFY
      ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY processed_date DESC,cutt_off_date DESC) = 1
-- where created_at > '2025-06-25 00:00:0.00 UTC'
)
select
  sited.*,
  num_factura_documento_ocr,
  ruc_emisor_path,
from  source_file 
inner join `{{project_id}}.genai_documents.auna_sited_mvp` as sited
on  source_file.id = sited.documento_id
;
-- SELECT COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_sited_mvp_sample`
-- SELECT DISTINCT num_factura_documento_ocr FROM `{{project_id}}.siniestro_salud_auna.auna_sited_mvp_sample`
-- SELECT DISTINCT processed_date FROM `{{project_id}}.siniestro_salud_auna.auna_sited_mvp_sample`




####################################################################################################
############################# PASO 2.1: Limpiar Fecha SITEDS #######################################
####################################################################################################
/*---------------------------------------------------------------
  CREA / REEMPLAZA tabla con fecha limpia
----------------------------------------------------------------*/
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean` AS

/* 1️⃣  tabla fuente ---------------------------------------------------- */
WITH base AS (
  SELECT
    *,
    CAST(metadata.fecha_hora_autorizacion AS STRING) AS fecha_raw
  FROM `{{project_id}}.siniestro_salud_auna.auna_sited_mvp_sample`
),

/* 2️⃣  pre-limpieza ---------------------------------------------------- */
prep AS (
  SELECT
    *,
    UPPER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(IFNULL(fecha_raw,''), r'[\n\r"]', ' '),
        r'\b(EN CLINICA|EN CLÍNICA|NPO|STRING|null)\b',
        ''
      )
    ) AS f0
  FROM base
), prep2 AS (
  SELECT *,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(f0, r'-', '/'),
        r'(\d{4})\.', r'\1 '
      ),
      r'\s*-\s*', ' '
    ) AS f1
  FROM prep
), prep3 AS (
  SELECT *,
    REGEXP_REPLACE(TRIM(f1), r'\s+', ' ') AS f_std
  FROM prep2
),

/* 3️⃣  parseo a TIMESTAMP (América/Lima) ------------------------------ */
parsed AS (
  SELECT
    *,
    CASE
      WHEN f_std = '' THEN NULL                                 -- ← fecha vacía
      ELSE
        COALESCE(
          SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S', f_std, 'America/Lima'),
          SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M',     f_std, 'America/Lima'),
          TIMESTAMP( SAFE.PARSE_DATE('%d/%m/%Y', f_std), 'America/Lima'),

          /* D/M/YY variantes ---------------------------------------- */
          SAFE.PARSE_TIMESTAMP(
            '%d/%m/%Y %H:%M:%S',
            REGEXP_REPLACE(f_std,
              r'^(\d{1,2}/\d{1,2})/(\d{2})(.*)$', r'\1/20\2\3'),
            'America/Lima'
          ),
          SAFE.PARSE_TIMESTAMP(
            '%d/%m/%Y %H:%M',
            REGEXP_REPLACE(f_std,
              r'^(\d{1,2}/\d{1,2})/(\d{2})(.*)$', r'\1/20\2\3'),
            'America/Lima'
          ),
          TIMESTAMP(
            SAFE.PARSE_DATE(
              '%d/%m/%Y',
              REGEXP_REPLACE(f_std,
                r'^(\d{1,2}/\d{1,2})/(\d{2})$', r'\1/20\2')
            ),
            'America/Lima'
          ),

          /* compacta 17022023 04 28 01 ------------------------------ */
          SAFE.PARSE_TIMESTAMP(
            '%d%m%Y%H %M %S',
            REGEXP_REPLACE(f_std,
              r'(\d{8})(\d{2}) (\d{2}) (\d{2})', r'\1\2 \3 \4'),
            'America/Lima'
          )
        )
    END AS ts_autorizacion
  FROM prep3
)

/* 4️⃣  tabla final ----------------------------------------------------- */
SELECT
  *,
  fecha_raw                                            AS fecha_hora_autorizacion_original,
  ts_autorizacion                                      AS fecha_hora_autorizacion_ts,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', ts_autorizacion)
      AS fecha_hora_autorizacion_iso,
  ts_autorizacion IS NULL                              AS flag_fecha_no_parseada
FROM parsed
;


------ VALIDACION FECHAS
-- SELECT DISTINCT fecha_hora_autorizacion_ts, fecha_hora_autorizacion_original
-- FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`
-- ;


##############################################################
################ PASO 2.2: Limpiar poliza del SITEDS ##########
##############################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v2` AS

/* 1️⃣  fuente + extracción del texto original ------------------------ */
WITH src AS (
  SELECT
    *,
    paciente.num_poliza AS poliza_raw
  FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`
),

/* 2️⃣  normaliza: quita blancos y signos : . , ----------------------- */
norm AS (
  SELECT
    *,
    REGEXP_REPLACE(
      REGEXP_REPLACE(IFNULL(poliza_raw, ''), r'\s+', ''),   -- sin espacios
      r'[:.,]',                                             -- sin : . ,
      ''
    ) AS poliza_tmp
  FROM src
)

/* 3️⃣  tabla final ---------------------------------------------------- */
SELECT
  * EXCEPT(poliza_raw, poliza_tmp),

  /* cadena vacía → NULL */
  CASE
    WHEN NULLIF(poliza_tmp, '') IS NULL THEN NULL
    WHEN REGEXP_CONTAINS(poliza_tmp, r'^[E]') THEN NULLIF(poliza_tmp, '')
    WHEN REGEXP_CONTAINS(poliza_tmp, r'^[0-9]') THEN NULLIF(poliza_tmp, '')
    ELSE NULL
  END AS poliza_limpia,

  /* flag: 0 = formato OK (4-15 A-Z, 0-9, guiones), 1 = inválida o NULL */
  CASE
    WHEN REGEXP_CONTAINS(poliza_tmp, r'^[A-Z0-9-]{3,15}$') THEN 0
    ELSE 1
  END AS flag_poliza_invalida

FROM norm
;


##########################################################################
################ PASO 2.3: Limpiar tipo de documento del SITEDS ##########
##########################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v3` AS

WITH base AS (
  SELECT
    *,
    paciente.tipo_documento AS tipo_doc_raw
  FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v2`
),

cleaned AS (
  SELECT
    *,
    -- Primero normalizamos el texto: eliminar espacios, puntos y convertir a mayúsculas
    UPPER(REGEXP_REPLACE(REGEXP_REPLACE(IFNULL(tipo_doc_raw, ''), r'[\s.]', ''), r'[^A-Z0-9]', '')) AS tipo_doc_clean
  FROM base
)

SELECT
  * EXCEPT(tipo_doc_raw, tipo_doc_clean),
  -- Aplicamos las condiciones para determinar el valor final
  CASE
    WHEN tipo_doc_clean = '' THEN NULL
    WHEN tipo_doc_clean = 'STRING' THEN NULL
    WHEN tipo_doc_clean = 'NA' THEN NULL
    WHEN tipo_doc_clean = 'N/A' THEN NULL
    ELSE tipo_doc_clean
  END AS tipo_documento_limpio,
  
  -- Flag para identificar documentos válidos (1=válido, 0=inválido)
  CASE
    WHEN tipo_doc_clean = '' THEN 0
    WHEN tipo_doc_clean = 'STRING' THEN 0
    WHEN tipo_doc_clean = 'NA' THEN 0
    WHEN tipo_doc_clean = 'N/A' THEN 0
    ELSE 1
  END AS flag_tipo_doc_valido

FROM cleaned;



##############################################################
################ PASO 2.4: Limpiar Codigo cmp del SITEDS ##############
##############################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v4` AS

WITH base AS (
  SELECT
    *,
    visacion_medico.codigo_cmp AS codigo_cmp_raw
  FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v3`
),

cleaned AS (
  SELECT
    *,
    -- Eliminar espacios, comas, puntos y cualquier letra (solo mantener números)
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        UPPER(IFNULL(codigo_cmp_raw, '')), 
        r'[\s,.]', ''),  -- Elimina espacios, comas y puntos
        r'[^0-9]', '')   -- Elimina todo lo que no sea dígito
    AS codigo_cmp_clean
  FROM base
)

SELECT
  * EXCEPT(codigo_cmp_raw, codigo_cmp_clean),
  -- Aplicar condiciones para determinar el valor final
  CASE
    WHEN codigo_cmp_clean = '' THEN NULL
    WHEN REGEXP_CONTAINS(UPPER(codigo_cmp_raw), r'STRING|NULL|N/A|NA') THEN NULL
    ELSE codigo_cmp_clean
  END AS codigo_cmp_limpio,
  
  -- Flag para identificar códigos válidos (1=válido, 0=inválido)
  CASE
    WHEN codigo_cmp_clean = '' THEN 0
    WHEN REGEXP_CONTAINS(UPPER(codigo_cmp_raw), r'STRING|NULL|N/A|NA') THEN 0
    ELSE 1
  END AS flag_codigo_cmp_valido

FROM cleaned
;



#######################################################################
################ PASO 2.5: Limpiar tipo_afiliacion del SITEDS #########
#######################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v5` AS

WITH base AS (
  SELECT
    *,
    titular.tipo_afiliacion AS tipo_afiliacion_raw
  FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v4`
),

cleaned AS (
  SELECT
    *,
    -- Normalizar texto: mayúsculas, eliminar espacios, puntos, comas y números
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        UPPER(IFNULL(tipo_afiliacion_raw, '')),
        r'[\s,.\d]', ''),  -- Elimina espacios, comas, puntos y números
      r'[^A-Z]', '')       -- Elimina cualquier caracter que no sea letra mayúscula
    AS tipo_afiliacion_clean
  FROM base
)

SELECT
  * EXCEPT(tipo_afiliacion_raw, tipo_afiliacion_clean),
  -- Aplicar condiciones para determinar el valor final
  CASE
    WHEN tipo_afiliacion_clean = '' THEN NULL
    WHEN LENGTH(tipo_afiliacion_clean) = 1 THEN NULL
    WHEN REGEXP_CONTAINS(tipo_afiliacion_clean, r'STRING|N/A|NA') THEN NULL
    WHEN REGEXP_CONTAINS(tipo_afiliacion_clean, r'REG') THEN 'REGULAR'
    WHEN REGEXP_CONTAINS(tipo_afiliacion_clean, r'POTEST') THEN 'POTESTATIVO'
    WHEN REGEXP_CONTAINS(tipo_afiliacion_clean, r'SCT') THEN 'SCTR'
    WHEN REGEXP_CONTAINS(tipo_afiliacion_clean, r'SOA') THEN 'SOAT'
    ELSE tipo_afiliacion_clean
  END AS tipo_afiliacion_limpio,
  
  -- Flag para identificar valores válidos (1=válido, 0=inválido)
  CASE
    WHEN tipo_afiliacion_clean = '' THEN 0
    WHEN LENGTH(tipo_afiliacion_clean) = 1 THEN 0
    WHEN REGEXP_CONTAINS(tipo_afiliacion_clean, r'STRING|N/A|NA') THEN 0
    ELSE 1
  END AS flag_tipo_afiliacion_valido

FROM cleaned
;


-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean` WHERE num_factura_documento_ocr = 'F116-00156943'
######################################################################################
################ PASO 2.6: Limpiar cobertura del SITEDS (usando trama) ###############
######################################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v6` AS

/* 1️⃣  Limpieza inicial de cobertura OCR ------------------------------*/
WITH ocr AS (
  SELECT
    *,
    LOWER(TRIM(beneficio_autorizado.nombre)) AS cobertura_raw
  FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v5`
),

cov_ocr AS (
  SELECT
    *,
    /* mapeo + nulls                                                   */
    CASE
      WHEN cobertura_raw IN ('n/a', 'na', 'string', '')   THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(LOWER(cobertura_raw)), r'ambulato')    THEN 'AMBULATORIO'
      WHEN REGEXP_CONTAINS(LOWER(cobertura_raw), r'onco')        THEN 'ONCOLOGIA'
      WHEN REGEXP_CONTAINS(LOWER(cobertura_raw), r'preve')       THEN 'PREVENCION'
      WHEN REGEXP_CONTAINS(LOWER(cobertura_raw), r'mater')       THEN 'MATERNIDAD'
      WHEN REGEXP_CONTAINS(LOWER(cobertura_raw), r'hospit')       THEN 'HOSPITALARIO' -- new -- Hay un caso F160-00008513 con cobertura ONCOLOGIA HOSPITALARIA
      WHEN REGEXP_CONTAINS(LOWER(cobertura_raw), r'emerg')       THEN 'EMERGENCIA' -- new
      -- 
      ELSE 'OTROS'
    END AS cobertura_tmp
  FROM ocr
),

/* 2️⃣  Traemos la cobertura de la trama SITED ------------------------*/
trama AS (
  SELECT
    num_factura_trama_sited,
    TIPOCOBERTURA
  FROM `{{project_id}}.siniestro_salud_auna.trama_sited_previa`
),

/* 3️⃣  Unimos OCR ↔ trama por número de factura ----------------------*/
joined AS (
  SELECT
    o.*,
    t.TIPOCOBERTURA
  FROM cov_ocr AS o
  LEFT JOIN trama AS t
    ON REPLACE(o.num_factura_documento_ocr, '-', '') = t.num_factura_trama_sited
)

/* 4️⃣  Cobertura final: OCR limpio, si no → mapeo de la trama --------*/
SELECT
  joined.*,
  CASE
      WHEN cobertura_tmp IS NOT NULL THEN cobertura_tmp         -- ya tiene valor
      ELSE
        CASE CAST(TIPOCOBERTURA AS INT64)
          WHEN 4                          THEN 'AMBULATORIO'
          WHEN 0  THEN 'OTROS'
          WHEN 1  THEN 'OTROS'
          WHEN 2  THEN 'OTROS'
          WHEN 3  THEN 'OTROS'
          WHEN 5  THEN 'OTROS'
          WHEN 6  THEN 'OTROS'
          WHEN 9  THEN 'OTROS'
          ELSE 'OTROS'
        END
  END AS cobertura_siteds_ocr_limpia
FROM joined
;

--------- VALIDACION COBERTURA
-- SELECT DISTINCT cobertura_siteds_ocr_limpia FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`




#############################################################################################
###### PASO 2.7: Añadir codigo producto de la trama (usando trama 1 factura) y producto #####
#############################################################################################
-- SELECT DISTINCT codigoproducto FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v7`
AS
SELECT
  A.*,
  B.codigoproducto,
  CASE 
    WHEN codigoproducto IN ('1', '62', '59') THEN 'AMC'
    WHEN codigoproducto IN ('S') THEN 'EPS'
    WHEN codigoproducto IN ('4', '11', '12', '38', '50', '52', '53', '95', 'E1', '7', '49', '54', '93', '2', '15', '28', '34', '16', '20') THEN 'AMI'
    ELSE 'OTROS'
  END AS producto_siteds_mod
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v6` AS A
LEFT JOIN(
  SELECT DISTINCT numero_de_documento_de_pago, ruc_tramas, codigoproducto FROM `{{project_id}}.siniestro_salud_auna.trama_factura_rfs`
) AS B
ON REPLACE(A.num_factura_documento_ocr, "-", "") = B.numero_de_documento_de_pago AND A.ruc_emisor_path = B.ruc_tramas
;


######################################################################################
############ PASO 2.8: Traer producto del SITEDS (usando tabla producto) #############
######################################################################################
-- CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`
-- AS
-- SELECT
--   *,
--   B.DESCRIPCION as producto_siteds_mod
-- FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean` AS A
-- LEFT JOIN `rs-prd-dlk-dd-rawzone-a40f.bdrsa__app_eps.TABLA_PRODUCTO` AS B
-- ON A.codigoproducto = B.COD_PRODUCTO
-- ;

-- SELECT * FROM `rs-prd-dlk-dd-rawzone-a40f.bdrsa__app_eps.TABLA_PRODUCTO`
-- SELECT DISTINCT codigoproducto, producto_siteds_mod FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`

-- 202401 0 - 202402 0 - 202403 1

######################################################################################
#################### PASO 2.9: Limpiar copago variable ###############################
######################################################################################
-- SELECT DISTINCT beneficio_autorizado.copago_variable FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`
-- Quiero limpiar el campo beneficio_autorizado.copago_variable y nombrarlo como cobertura_copago_variable_clean, tiene valores como 'CUBIERTO AL 65%'. La idea sería identificar a los que tengan un '%' y quedarse con el número (ejemplo 'CUBIERTO AL 65%' sería 0.65 (porque sería como número el porcentaje en lugar de 65))
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v8` AS
SELECT
  *,
  
  -- Limpieza de copago variable: extraer porcentaje y convertir a decimal
  CASE
    WHEN REGEXP_CONTAINS(beneficio_autorizado.copago_variable, r'%') THEN 
      SAFE_CAST(REGEXP_EXTRACT(beneficio_autorizado.copago_variable, r'(\d+)%') AS FLOAT64) / 100
    ELSE NULL
  END AS cobertura_copago_variable_clean

FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v7`
;

--SELECT DISTINCT cobertura_copago_variable_clean FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`


######################################################################################
#################### PASO 2.10: Limpiar copago fijo ##################################
######################################################################################
-- SELECT DISTINCT beneficio_autorizado.copago_fijo FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`
-- Quiero limpiar el campo beneficio_autorizado.copago_fijo (es un string) y nombrarlo como copago_fijo_clean. La idea sería que en caso no haya ningún número sea null, en caso contenga 'cuarto' (ya sea en minúsculas o mayúsculas) sea null. El valor resultante debe ser el número (ejemplo: 35.00 SOLES POR, sería 35; Ninguna sería null; 1 DIA DE CUARTO sería null)
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v9` AS
SELECT
  *,
  -- Limpieza de copago fijo
  CASE
    WHEN beneficio_autorizado.copago_fijo IS NULL THEN NULL
    WHEN REGEXP_CONTAINS(LOWER(beneficio_autorizado.copago_fijo), r'cuarto') THEN NULL
    WHEN REGEXP_EXTRACT(beneficio_autorizado.copago_fijo, r'\d+(?:\.\d+)?') IS NULL THEN NULL
    ELSE SAFE_CAST(REGEXP_EXTRACT(beneficio_autorizado.copago_fijo, r'\d+(?:\.\d+)?') AS FLOAT64)
  END AS copago_fijo_clean

FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v8`
;




----------- VALIDACION copago_fijo_clean
-- SELECT DISTINCT copago_fijo_clean FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`



######################################################################################
################ PASO 2.11: Limpiar Razon social contratante #########################
######################################################################################
-- SELECT DISTINCT titular.contratante FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean`
-- 
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v10` AS
SELECT
  *,
  titular.contratante as razon_social_contratante_siteds_ocr
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v9`
;



######################################################################################
################## PASO 2.12: Limpiar CIE10/diagnostico fijo #########################
######################################################################################
-- Valores raros
# Considera que diagnosticos.codigo es tipo RECORD
# Crea un nuevo campo llamado cie10_siteds_ocr, si es que un registro tiene varios codigo, ya que es tipo record, agarra el primero que encuentres
# Eliminar los puntos y comas ('.', ',')
# Si el valor es %CIE10%: dejarlo como null
# Si el valor es %N/A%: dejarlo como null
# Si tiene 4 números y el 1er número es '1' entonces reemplazar ese primer número por 'I'
# Si tiene 4 números y el 1er número es '0' entonces reemplazar ese primer número por 'O'

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v11` AS
SELECT
  t.*,
  CASE 
    WHEN REGEXP_CONTAINS(TRIM(REGEXP_REPLACE(d.codigo, r'[.,]', '')), r'CIE10|N/A') THEN NULL
    WHEN REGEXP_CONTAINS(TRIM(REGEXP_REPLACE(d.codigo, r'[.,]', '')), r'^1\d{3}$') 
      THEN CONCAT('I', SUBSTR(TRIM(REGEXP_REPLACE(d.codigo, r'[.,]', '')), 2))
    WHEN REGEXP_CONTAINS(TRIM(REGEXP_REPLACE(d.codigo, r'[.,]', '')), r'^0\d{3}$') 
      THEN CONCAT('O', SUBSTR(TRIM(REGEXP_REPLACE(d.codigo, r'[.,]', '')), 2))
    ELSE TRIM(REGEXP_REPLACE(d.codigo, r'[.,]', ''))
  END AS cie10_siteds_ocr
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v10` AS t
LEFT JOIN UNNEST(t.diagnosticos) AS d WITH OFFSET AS pos
ON TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.num_factura_documento_ocr ORDER BY pos) = 1
;




###################################################################################################
########### PASO 03: Crear tabla de campos necesarios del OCR de la hoja SITEDS ###################
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds`
AS
SELECT 
  DISTINCT
    --REGEXP_EXTRACT(pdf_id, r'F\d+_\d+') AS num_factura,
    page_path,
    num_factura_documento_ocr as num_factura_siteds_ocr,
    ruc_emisor_path,
    --REPLACE(REGEXP_EXTRACT(page_path, r'F\d+_\d+'), "_", "-") as  num_factura_siteds_ocr,
    cabecera.numero_orden as num_siteds_ocr,
    --beneficio_autorizado.nombre as cobertura_siteds_ocr,
    cobertura_siteds_ocr_limpia,
    codigoproducto as codigoproducto_trama,
    producto_siteds_mod,
    cabecera.producto as producto_siteds_ocr,
    --producto_siteds_ocr_clean as producto_siteds_ocr,
    paciente.num_documento as num_documento_paciente_siteds_ocr,
    cobertura_copago_variable_clean as cobertura_copago_variable_siteds_ocr,
    copago_fijo_clean as copago_fijo_siteds_ocr,
    DATETIME(fecha_hora_autorizacion_ts, 'America/Lima') AS fecha_emision_siteds_ocr,
    razon_social_contratante_siteds_ocr,
    --DATE(PARSE_DATETIME('%d/%m/%Y %H:%M:%S', `metadata`.fecha_hora_autorizacion)) AS fecha_emision_siteds_ocr,
    cie10_siteds_ocr,
    poliza_limpia as num_poliza_siteds_ocr, 
    -- Estos son campos para luego hacer el comparativo
    tipo_documento_limpio,
    codigo_cmp_limpio,
    tipo_afiliacion_limpio as tipo_afiliacion_ocr,
     -- select distinct paciente.num_poliza

     -- FLAGS
     flag_poliza_invalida,
     flag_tipo_doc_valido,
     flag_codigo_cmp_valido,
     flag_tipo_afiliacion_valido
  FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_clean_v11`
  --UNNEST(entidades.diagnosticos) AS d
  LEFT JOIN UNNEST(diagnosticos) AS d
  ON TRUE
;







###################################################################################################
############ PASO 04 (CASI FINAL): Ajustar campos del OCR Siteds con campos de Trama SITEDS #######
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_of`
AS
SELECT 
  A.* EXCEPT(num_siteds_ocr, num_documento_paciente_siteds_ocr),
  
  -- Limpieza y validación de num_siteds_ocr_val
  CASE
    WHEN A.num_siteds_ocr IS NULL AND B.numero_del_documento_de_autorizacion IS NULL THEN NULL
    WHEN A.num_siteds_ocr IS NULL THEN 
      /*CASE
        WHEN REGEXP_CONTAINS(UPPER(B.numero_del_documento_de_autorizacion), r'STRING|N/A|NA|^$') THEN NULL
        ELSE REGEXP_REPLACE(B.numero_del_documento_de_autorizacion, r'[^0-9]', '')
      END*/ B.numero_del_documento_de_autorizacion
    ELSE 
      CASE
        WHEN REGEXP_CONTAINS(UPPER(A.num_siteds_ocr), r'STRING|N/A|NA|^$') THEN NULL
        ELSE REGEXP_REPLACE(A.num_siteds_ocr, r'[^0-9]', '')
      END
  END AS num_siteds_ocr_val,
  
  -- Limpieza y validación de num_documento_paciente_siteds_ocr_val
  CASE
    WHEN A.num_documento_paciente_siteds_ocr IS NULL AND B.numero_del_documento_de_identidad IS NULL THEN NULL
    WHEN A.num_documento_paciente_siteds_ocr IS NULL THEN 
      /*CASE
        WHEN REGEXP_CONTAINS(UPPER(B.numero_del_documento_de_identidad), r'STRING|N/A|NA|^$') THEN NULL
        ELSE REGEXP_REPLACE(B.numero_del_documento_de_identidad, r'[^0-9]', '')
      END*/ B.numero_del_documento_de_identidad
    ELSE 
      CASE
        WHEN REGEXP_CONTAINS(UPPER(A.num_documento_paciente_siteds_ocr), r'STRING|N/A|NA|^$') THEN NULL
        ELSE REGEXP_REPLACE(A.num_documento_paciente_siteds_ocr, r'[^0-9]', '')
      END
  END AS num_documento_paciente_siteds_ocr_val,
  
  -- Flag de validación para documento del paciente (1 = INVÁLIDO, 0 = VÁLIDO)
  CASE
    WHEN (A.num_documento_paciente_siteds_ocr IS NULL AND B.numero_del_documento_de_identidad IS NULL) THEN 1
    WHEN A.num_documento_paciente_siteds_ocr IS NULL THEN 
      CASE
        WHEN REGEXP_CONTAINS(UPPER(B.numero_del_documento_de_identidad), r'STRING|N/A|NA|^$') THEN 1
        WHEN REGEXP_REPLACE(B.numero_del_documento_de_identidad, r'[^0-9]', '') = '' THEN 1
        ELSE 0
      END
    ELSE 
      CASE
        WHEN REGEXP_CONTAINS(UPPER(A.num_documento_paciente_siteds_ocr), r'STRING|N/A|NA|^$') THEN 1
        WHEN REGEXP_REPLACE(A.num_documento_paciente_siteds_ocr, r'[^0-9]', '') = '' THEN 1
        ELSE 0
      END
  END AS flag_doc_paciente_siteds_invalido,
  B.fecha_de_prestacion,
  
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds` AS A
LEFT JOIN `{{project_id}}.siniestro_salud_auna.trama_sited_previa` AS B
ON (REPLACE(A.num_factura_siteds_ocr, '-', '') = B.num_factura_trama_sited AND A.ruc_emisor_path = B.RUCIPRESS)
;





###################################################################################################
################# PASO 05 (FINAL): Quedarnos con facturas únicas en SITEDS OCR ####################
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`
AS WITH
tabla_ini AS (
select
*

/*from `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_Katherin`*/
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_of`
 
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY num_factura_siteds_ocr ORDER BY
      --copago_fijo_siteds_ocr IS NULL,
      fecha_emision_siteds_ocr IS NULL,
      fecha_emision_siteds_ocr asc) = 1
)
SELECT 
  *
FROM tabla_ini
;


-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico` WHERE num_factura_siteds_ocr = 'F740-00023449' -- 20100251176
-- 20100251176



###################################################################################################
#################### PASO 06: Quedarnos con facturas CPM en SITEDS OCR ############################
###################################################################################################
-- Nos quedamos con todas las hojas siteds de las facturas netamente CPM
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_cpm`
AS
SELECT 
  A.* 
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_of` AS A
LEFT JOIN `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion` AS B
ON A.num_factura_siteds_ocr = B.num_factura_liqui_ocr AND A.ruc_emisor_path = B.ruc_liqui_ocr
WHERE B.mecanismo_liqui_ocr = 'CPM' 
;



###################################################################################################
##################### PASO 07 (OPCIONAL): Analisis de nulos en SITEDS OCR #########################
###################################################################################################

SELECT 
  'page_path' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(page_path) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'num_factura_siteds_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(num_factura_siteds_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'cobertura_siteds_ocr_limpia' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(cobertura_siteds_ocr_limpia) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'producto_siteds_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(producto_siteds_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'fecha_emision_siteds_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(fecha_emision_siteds_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'cie10_siteds_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(cie10_siteds_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'num_poliza_siteds_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(num_poliza_siteds_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'tipo_documento_limpio' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(tipo_documento_limpio) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'codigo_cmp_limpio' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(codigo_cmp_limpio) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'tipo_afiliacion_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(tipo_afiliacion_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'flag_poliza_invalida' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(flag_poliza_invalida) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'flag_tipo_doc_valido' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(flag_tipo_doc_valido) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'flag_codigo_cmp_valido' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(flag_codigo_cmp_valido) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'flag_tipo_afiliacion_valido' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(flag_tipo_afiliacion_valido) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'num_siteds_ocr_val' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(num_siteds_ocr_val) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'num_documento_paciente_siteds_ocr_val' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(num_documento_paciente_siteds_ocr_val) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'flag_doc_paciente_siteds_invalido' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(flag_doc_paciente_siteds_invalido) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`

UNION ALL SELECT 
  'cobertura_copago_variable_siteds_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(cobertura_copago_variable_siteds_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico`


ORDER BY cantidad_nulos DESC
;


