################## 01_FAB_SS_Factura_Unica ###########################

# Nombre Query: 01_FAB_SS_Factura_Unica
# Objetivo: Identificar la factura única en el OCR desde la fecha solicitada
# Objetivos: 
# - O1: Limpiar las fechas (calidad de datos)
# - O2: Delimitar los casos según fecha de emisión solicitada
# - O2: Obtener la factura única (fecha más reciente)

######################################################################


####################### FUENTES ###########################
-- 1. `{{project_id}}.genai_documents.auna_documents`
-- 2. `{{project_id}}.genai_documents.auna_invoice_mvp`
-- 3. `{{project_id}}.siniestro_salud_auna.trama_sited_previa` # Esto se creó anteriormente en la querie de trama siteds
-- 4. `{{project_id}}.siniestro_salud_auna.trama_factura_rfs` # Esto se creó anteriormente en la querie de trama factura

######################################################################

##############################################################
################ PASO 00: TEMPORAL USAR CIERTAS FACTURAS  ####
##############################################################
-- created_at: Fecha de cuándo los agentes de los MLEs inician sus procesos
-- cutt_off_date: hora a la que llega el lote
-- batch_name: número de lote
-- application_name: no sé
-- el ruc del proveedor no lo están sacando (se puede sacar desde file_path)
-- cutt_off_date
-- processed_date

DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);


CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_invoice_sample` AS

/* 1️⃣  archivo fuente (una fila por invoice_number) ------------------ */
WITH source_file AS (
  SELECT
    *,
    invoice_number AS num_factura_documento_ocr,
    --REPLACE(REGEXP_EXTRACT(file_path, r'F\d+_\d+'), "_", "-") as  num_factura_documento_ocr,
    
    -- RUC = primeros 11 dígitos del nombre del archivo
    REGEXP_EXTRACT(file_name, r'^(\d{11})') AS ruc_emisor_path -- select distinct processed_date
  FROM `{{project_id}}.genai_documents.auna_documents`
  -- ESTO SERÁ TEMPORAL
  WHERE processed_date = PERIODO_INI --'2025-07-09' AND '2025-07-19' 
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY invoice_number
                       ORDER BY processed_date DESC, cutt_off_date DESC) = 1
)

/* 2️⃣  unión con la tabla de invoices ------------------------------- */
SELECT
  sf.processed_date,
  inv.*,
  sf.num_factura_documento_ocr,
  sf.ruc_emisor_path,                 -- ← nuevo campo
FROM   source_file AS sf
JOIN   `{{project_id}}.genai_documents.auna_invoice_mvp` AS inv
  ON sf.id = inv.documento_id
;


# Cuántas facturas hay en GCP
-- SELECT processed_date, count(DISTINCT invoice_number) FROM `{{project_id}}.genai_documents.auna_documents` GROUP BY ALL ORDER BY 1 DESC, 2 ASC ;


# ¿Cuántas facturas cruzaron?
-- SELECT processed_date, COUNT(DISTINCT num_factura_documento_ocr) FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_sample` GROUP BY ALL ORDER BY 1 DESC


-- SELECT * FROM `{{project_id}}.genai_documents.auna_documents`
-- WHERE TRUE
-- AND processed_date BETWEEN '2025-09-16' AND '2025-09-19'
-- AND invoice_number NOT IN (
--   SELECT DISTINCT num_factura_documento_ocr FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_sample`
-- )


##############################################################
################ PASO 01: Tabla con fechas limpias ###########
##############################################################

/*---------------------------------------------------------------
  TABLA CON TODAS LAS FILAS + FECHA LIMPIA
----------------------------------------------------------------*/
-- SELECT DISTINCT fecha_emision_fact_limpio,fecha_emision_original  FROM {{project_id}}.genai_documents.auna_invoice_mvp_clean where fecha_emision_fact_limpio is null and fecha_emision_original is not null

-- SELECT * FROM {{project_id}}.genai_documents.auna_invoice_mvp_clean where fecha_emision_original like '%Juní%'

CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean` AS

/* ───────── 1. Normalizar texto ───────── */
WITH et01 AS (
  SELECT
    *,
    -- a) normaliza NFD  b) quita tildes
    -- c) corrige JUNÍA/Junia/JUNia → JUNIO con (?i)  d) MAYÚSCULAS
    UPPER(
      REGEXP_REPLACE(                         -- c)
        REGEXP_REPLACE(                       -- b)
          NORMALIZE(cabecera.fecha_emision, NFD),   -- a)
          r'\p{M}', ''
        ),
        r'(?i)JUNIA',   -- ← patrón insensible a mayúsc./minúsc.
        'JUNIO'
      )
    ) AS fecha_norm
  FROM {{project_id}}.siniestro_salud_auna.auna_invoice_sample -- BASE
),

/* ───────── 2. Limpiar prefijos y hora ───────── */
et02 AS (
  SELECT *,
    REGEXP_REPLACE(fecha_norm,
      r'^(LUNES|MARTES|MIERCOLES|MIÉRCOLES|JUEVES|VIERNES|SABADO|SÁBADO|DOMINGO),\s*',
      '') AS sin_prefijo
  FROM et01
), 
/* ───────── 2-bis. Limpiar hora con o sin coma ───────── */
et03 AS (
  SELECT
    *,
    -- quita “, HH:MM”, “ HH:MM” o “ HH:MM:SS”
    REGEXP_REPLACE(
      sin_prefijo,
      r'\s*,?\s*\d{1,2}:\d{2}(:\d{2})?.*$',      -- ← coma opcional
      ''
    ) AS sin_hora
  FROM et02
),
et04 AS (
  SELECT *,
    REGEXP_REPLACE(sin_hora,
      r'\s*(\.DE| DEL | DE )\s*', ' ') AS sin_conectores
  FROM et03
),
/* ───────── 5. Texto limpio (colapsa espacios) ───────── */
et05 AS (
  SELECT
    *,
    -- trim final + elimina punto/ : + colapsa espacios múltiples
    REGEXP_REPLACE(
      TRIM(REGEXP_REPLACE(sin_conectores, r'[.:]$', '')),
      r'\s+',
      ' '
    ) AS texto_limpio
  FROM et04
),

/* ───────── 3. Construir fecha (DATE) ───────── */
final AS (
  SELECT
    *,
    CASE
      WHEN REGEXP_CONTAINS(texto_limpio, r'^\d{1,2}/\d{1,2}/\d{2,4}$') THEN
        SAFE.PARSE_DATE(
          '%d/%m/%Y',
          REGEXP_REPLACE(texto_limpio,
            r'^(\d{1,2}/\d{1,2})/(\d{2})$', r'\1/20\2')
        )
      WHEN REGEXP_CONTAINS(texto_limpio, r'^\d{4}-\d{2}-\d{2}$') THEN
        SAFE.PARSE_DATE('%Y-%m-%d', texto_limpio)
      WHEN REGEXP_CONTAINS(texto_limpio, r'^\d{1,2} [A-Z]+ \d{4}$') THEN (
        SELECT DATE(
          CAST(REGEXP_EXTRACT(texto_limpio, r'(\d{4})$') AS INT64),
          CASE REGEXP_EXTRACT(texto_limpio, r'[A-Z]+')
            WHEN 'ENERO'      THEN 1  WHEN 'FEBRERO'    THEN 2
            WHEN 'MARZO'      THEN 3  WHEN 'ABRIL'      THEN 4
            WHEN 'MAYO'       THEN 5  WHEN 'JUNIO'      THEN 6
            WHEN 'JULIO'      THEN 7  WHEN 'AGOSTO'     THEN 8
            WHEN 'SEPTIEMBRE' THEN 9  WHEN 'SETIEMBRE'  THEN 9
            WHEN 'OCTUBRE'    THEN 10 WHEN 'NOVIEMBRE'  THEN 11
            WHEN 'DICIEMBRE'  THEN 12
          END,
          CAST(REGEXP_EXTRACT(texto_limpio, r'^(\d{1,2})') AS INT64)
        )
      )
      ELSE NULL
    END AS fecha_emision_fact_limpio
  FROM et05
)

/* ───────── 4. Tabla definitiva ───────── */
SELECT
  -- ①  Mantenemos la fecha original como columna independiente
  cabecera.fecha_emision                        AS fecha_emision_original,

  -- ②  Todos los campos menos los temporales
  * EXCEPT(fecha_norm, sin_prefijo, sin_hora,
           sin_conectores, texto_limpio),

  -- ③  Bandera y derivadas
  fecha_emision_fact_limpio IS NULL             AS flag_fecha_sin_parsear,
  EXTRACT(YEAR  FROM fecha_emision_fact_limpio) AS anio_emision,
  EXTRACT(MONTH FROM fecha_emision_fact_limpio) AS mes_emision,
  FORMAT_DATE('%B', fecha_emision_fact_limpio)  AS mes_nombre,
  FORMAT_DATE('%A', fecha_emision_fact_limpio)  AS dia_semana
FROM final
;


----- Validacion de Fechas
-- SELECT DISTINCT fecha_emision_fact_limpio FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean`


##############################################################
################ PASO 02: Limpiar RUC ########################
##############################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean_v1` AS

SELECT
  *,
  -- ① primera coincidencia de 11 dígitos  →  STRING
  --REGEXP_EXTRACT(emisor.ruc, r'(\d{11})')     AS ruc_clean,
  REGEXP_EXTRACT(ruc_emisor_path, r'(\d{11})')     AS ruc_clean,
  
  -- opcional: bandera para saber si NO se halló un RUC válido
  --REGEXP_EXTRACT(emisor.ruc, r'(\d{11})') IS NULL  AS flag_ruc_invalido
  REGEXP_EXTRACT(ruc_emisor_path, r'(\d{11})') IS NULL  AS flag_ruc_invalido

FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean`
;


---------- Validacion RUC
-- SELECT COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean`
-- SELECT DISTINCT ruc_clean, count(*) FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean` GROUP BY ALL


#############################################################################
############ PASO 03: Limpiar codigo de autorizacion (usando trama) #########
#############################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean_v2` AS

WITH src AS (
  -- 1️⃣  OCR: extraigo dígitos del código de autorización
  SELECT
    *,
    REGEXP_REPLACE(IFNULL(paciente.codigo_autorizacion,''), r'[^0-9]', '')
        AS cod_digits_ocr
  FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean_v1`
),

trama AS (
  -- 2️⃣  Trama: dígitos del doc. de autorización
  SELECT
    RUCIPRESS,
    num_factura_trama_sited,
    numero_del_documento_de_autorizacion as trama_aut_digits,
    -- REGEXP_REPLACE(numero_del_documento_de_autorizacion, r'[^0-9]', '') AS trama_aut_digits
  FROM `{{project_id}}.siniestro_salud_auna.trama_sited_previa`
),

joined AS (
  -- 3️⃣  Join por factura (sin guion en OCR)
  SELECT
    s.*,
    t.trama_aut_digits
  FROM src AS s
  LEFT JOIN trama AS t
    ON REPLACE(s.num_factura_documento_ocr, '-', '') = t.num_factura_trama_sited AND s.ruc_clean = t.RUCIPRESS
)

-- 4️⃣  Tabla final: añadimos columnas DEFINITIVAS
SELECT
  joined.*,
  -- ---------- código limpio definitivo --------------------------
  CASE
    WHEN LENGTH(cod_digits_ocr) = 10           THEN cod_digits_ocr
    WHEN LENGTH(trama_aut_digits) <> 10         THEN trama_aut_digits
    ELSE NULL
  END AS codigo_autorizacion_limpio,

FROM joined
;

-- SELECT * 
-- SELECT COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean`
-- 2296 (OBS: Se ha expandido la base en unos 60 casos aprox, habría que averiguar por qué)


---------- Validacion Cod autorizacion (codigo siteds)
-- 
-- SELECT DISTINCT codigo_autorizacion_limpio, COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean` GROUP BY ALL ORDER BY 2 DESC
-- SELECT num_factura_documento_ocr, cod_digits_ocr,trama_aut_digits, * FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean` WHERE codigo_autorizacion_limpio is null


-- ¿Por qué hay codigos de autorizacion nulos? Revisar esas facturas en la trama
-- SELECT * FROM WHERE num_factura_trama_sited = 


##############################################################
################ PASO 04: Limpiar poliza de la factura #######
##############################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean_v3` AS

/* 1️⃣  fuente + extracción del texto original ------------------------ */
WITH src AS (
  SELECT
    * ,
    info_seguro.poliza AS poliza_raw
  FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean_v2`
),

/* 2️⃣  normaliza: quita blancos y signos : . , ----------------------- */
norm AS (
  SELECT
    * ,
    REGEXP_REPLACE(
      REGEXP_REPLACE(IFNULL(poliza_raw, ''), r'\s+', ''),   -- sin espacios
      r'[:.,]',                                              -- sin : . ,
      ''
    ) AS poliza_tmp
  FROM src
)

/* 3️⃣  tabla final ---------------------------------------------------- */
SELECT
  * EXCEPT(poliza_raw, poliza_tmp),

  /* cadena vacía → NULL */
  
  CASE
    WHEN REGEXP_CONTAINS(UPPER(poliza_tmp), r'[A-Z]{2}') THEN NULL
    ELSE NULLIF(poliza_tmp, '')
  END AS poliza_limpia,

  /* flag: 0 = formato OK, 1 = inválida o NULL o contiene 2 letras seguidas */
  CASE
    WHEN REGEXP_CONTAINS(UPPER(poliza_tmp), r'[A-Z]{2}') THEN 1
    WHEN REGEXP_CONTAINS(poliza_tmp, r'^[A-Z0-9-]{3,15}$') THEN 0
    ELSE 1
  END AS flag_poliza_invalida

FROM norm
;


-- SELECT COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean`
-- 2296
-- SELECT DISTINCT info_seguro.poliza FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean`
-- SELECT DISTINCT poliza_limpia FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean`



####################################################################################
################ PASO 05: Limpiar dni de la factura (usando trama siteds) ##########
####################################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean_v4` AS

/* 1️⃣  OCR: conservar solo dígitos ------------------------------------*/
WITH ocr AS (
  SELECT
    *,
    REGEXP_REPLACE(                           -- quita espacios, :, ., ,, -, etc.
      REGEXP_REPLACE(IFNULL(paciente.dni, ''), r'[\s:.,\-]', ''),
      r'[^0-9]', ''
    ) AS dni_digits_ocr
  FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean_v3`
),

/* 2️⃣  Trama: se trae el valor RAW, sin limpiar ------------------------*/
trama AS (
  SELECT
    RUCIPRESS,
    num_factura_trama_sited,
    numero_del_documento_de_identidad AS dni_raw_trama      -- ← sin cambios
  FROM `{{project_id}}.siniestro_salud_auna.trama_sited_previa`
),

/* 3️⃣  Join OCR ↔ Trama por factura (sin guion en el OCR) --------------*/
joined AS (
  SELECT
    o.*,
    t.dni_raw_trama
  FROM ocr AS o
  LEFT JOIN trama AS t
    ON REPLACE(o.num_factura_documento_ocr, '-', '') = t.num_factura_trama_sited and o.ruc_clean = t.RUCIPRESS
)

/* 4️⃣  Valor definitivo + flag ----------------------------------------*/
SELECT
  joined.*,

  /* -------- DNI limpio/final ---------------------------------------*/
  CASE
    WHEN REGEXP_CONTAINS(dni_digits_ocr, r'^[0-9]{8}$')
         THEN dni_digits_ocr                 -- OCR válido
    ELSE NULLIF(TRIM(dni_raw_trama), '')     -- si no, el valor crudo de la trama
  END AS dni_factura_limpio,

  /* -------- flag: 0 = 8 dígitos, 1 = otro/NULL ---------------------*/
  CASE
    WHEN REGEXP_CONTAINS(
           COALESCE(
             CASE
               WHEN REGEXP_CONTAINS(dni_digits_ocr, r'^[0-9]{8}$')
                    THEN dni_digits_ocr
               ELSE NULL
             END,
             NULLIF(TRIM(dni_raw_trama), '')
           ),
         r'^[0-9]{8}$')
      THEN 0
    ELSE 1
  END AS flag_dni_invalido

FROM joined
;



##############################################################
######### PASO 06: Creacion flag factura coincide ############
##############################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited` AS

/* 1️⃣  traemos solo lo que hace falta y limpiamos espacios ---------- */
WITH norm AS (
  SELECT
    *,
    REGEXP_REPLACE(IFNULL(cabecera.numero_factura,       ''), r'\s+', '') AS src_raw,
    REGEXP_REPLACE(IFNULL(num_factura_documento_ocr,     ''), r'\s+', '') AS ocr_raw
  FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_clean_v4`
),

/* 2️⃣  insertamos guion si falta tras los 4 primeros caracteres ------ */
ready AS (
  SELECT
    *,
    CASE
      WHEN src_raw = ''         THEN NULL
      WHEN STRPOS(src_raw,'-') > 0
           THEN src_raw
      ELSE CONCAT(SUBSTR(src_raw,1,4), '-', SUBSTR(src_raw,5))
    END AS src_norm,

    CASE
      WHEN ocr_raw = ''         THEN NULL
      WHEN STRPOS(ocr_raw,'-') > 0
           THEN ocr_raw
      ELSE CONCAT(SUBSTR(ocr_raw,1,4), '-', SUBSTR(ocr_raw,5))
    END AS ocr_norm
  FROM norm
)

/* 3️⃣  tabla final con flag ----------------------------------------- */
SELECT
  * EXCEPT(src_raw, ocr_raw, src_norm, ocr_norm),

  src_norm AS numero_factura_src_normalizado,
  ocr_norm AS numero_factura_ocr_normalizado,

  CASE
    WHEN src_norm IS NULL OR ocr_norm IS NULL                       THEN 0
    WHEN src_norm LIKE CONCAT('%', ocr_norm, '%')                   THEN 1
    WHEN ocr_norm LIKE CONCAT('%', src_norm, '%')                   THEN 1
    ELSE 0
  END AS flag_fact_coincide
FROM ready
;



##############################################################
############ PASO 07: Razon Social - Limpieza ################
##############################################################
-- SELECT DISTINCT emisor.nombre FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_sample`
# Quiero limpiar el campo emisor.nombre y crear uno nuevo en llamado razon_social_fact_limpia_ocr. Para hacerlo, primero eliminar ('.', ',') en los valores. Luego, lo que diga textualmente 'null' o valor vacío ('') asignarle null. 
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited_v1` 
AS
SELECT 
  *,
  CASE 
    WHEN TRIM(REPLACE(REPLACE(emisor.nombre, '.', ''), ',', '')) IN ('', 'null') THEN NULL
    WHEN TRIM(REPLACE(REPLACE(emisor.nombre, '.', ''), ',', '')) LIKE '%VALLE SUR%' THEN 'CLINICA VALLESUR SA'
    ELSE TRIM(REPLACE(REPLACE(emisor.nombre, '.', ''), ',', ''))
  END AS razon_social_fact_limpia_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited`
;



##############################################################
########## PASO 08: Items.descripcion - Limpieza #############
##############################################################
-- SELECT DISTINCT it.descripcion FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited`, UNNEST(items) as it

-- Paso 1: Crear una tabla auxiliar con el flag por factura
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.tmp_flag_medicamento_por_factura` AS
SELECT 
  base.num_factura_documento_ocr,  -- Ajusta este campo si el identificador de factura tiene otro nombre
  MAX(CASE 
    WHEN UPPER(it.descripcion) LIKE '%MEDICAMENTO%' THEN 1 ELSE 0
  END) AS flag_medicamento_factura_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited_v1` AS base,
UNNEST(base.items) AS it
GROUP BY base.num_factura_documento_ocr
;

-- Paso 2: Unir el flag a la tabla original
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited_cleaned` AS
SELECT 
  base.*,
  tmp.flag_medicamento_factura_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited_v1` AS base
LEFT JOIN `{{project_id}}.siniestro_salud_auna.tmp_flag_medicamento_por_factura` AS tmp
ON base.num_factura_documento_ocr = tmp.num_factura_documento_ocr
;

-- SELECT DISTINCT num_factura_documento_ocr, flag_medicamento_factura_ocr FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited_cleaned`

-- SELECT it.descripcion, COUNT(*) 
-- FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_sample` 
-- LEFT JOIN UNNEST(items) as it 
-- WHERE it.descripcion LIKE '%LABORATORIO%'
-- GROUP BY ALL 
-- ORDER BY 2 DESC 


###################################################################
############ PASO 09: RUC RIMAC COMPANIA - Limpieza ###############
###################################################################
-- SELECT DISTINCT emisor.nombre FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_sample`
# Quiero limpiar el campo receptor.ruc, y crear uno nuevo llamado ruc_compania_factura_ocr. Para hacerlo, primero eliminar ('.', ',', ':' o espacios) en los valores. Luego, lo que diga textualmente 'null' o valor vacío ('') asignarle null. 
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited_cleaned_v1`
AS
SELECT 
  *,
  CASE 
    WHEN TRIM(REGEXP_REPLACE(receptor.ruc, r'[.,: ]', '')) IN ('', 'null') THEN NULL
    ELSE TRIM(REGEXP_REPLACE(receptor.ruc, r'[.,: ]', ''))
  END AS ruc_compania_factura_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited_cleaned`
;






##############################################################
################ PASO 10: Tabla con factura única ############
##############################################################

-- 255
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_last`
AS
SELECT *
FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_limited_cleaned_v1`
QUALIFY
      ROW_NUMBER() OVER (PARTITION BY num_factura_documento_ocr ORDER BY flag_fact_coincide DESC,fecha_emision_fact_limpio DESC) = 1
;




##############################################################
################ PASO 11: TEMPORAL QUEDARSE CON FLAG = 1 ####
##############################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_last_of`
AS
SELECT 
  *
FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_last`
-- WHERE flag_fact_coincide = 1
;



###################################################################################################
########### PASO 12 (CASI FINAL): Limpiar el monto de la factura (usando la trama) ################
###################################################################################################

------- CREAMOS TABLA DE FACTURA CON MONTO LIMPIO
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_last_of_v1`
as
 
with tabla_ini as (
  SELECT a.*,
  CASE 
    WHEN b.subtotal_tramas IS NOT NULL AND (a.totales.subtotal = 0 OR a.totales.subtotal IS NULL) THEN b.subtotal_tramas ELSE a.totales.subtotal END 
  AS subtotal_ocr_val,
  CASE 
    WHEN b.importe_total_tramas IS NOT NULL AND (a.totales.importe_total = 0 OR a.totales.importe_total IS NULL) THEN b.importe_total_tramas ELSE a.totales.importe_total END 
  AS total_ocr_val,
  b.importe_total_tramas,
  CASE 
    WHEN a.fecha_emision_fact_limpio IS NULL THEN CAST(b.fecha_emision_tramas AS DATE) ELSE a.fecha_emision_fact_limpio END
  AS fecha_emision_fact_limpio_val,
--select *
  from `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_last_of`  a
--  limit 10
 
  left join `{{project_id}}.siniestro_salud_auna.trama_factura_rfs` b
  on (
    REPLACE(a.num_factura_documento_ocr, '-', '')  = b.numero_de_documento_de_pago AND A.ruc_clean = B.RUC_TRAMAS
  )
)
select *
from tabla_ini
;




###################################################################################################
########### PASO 13 (FINAL): Crear tabla de campos necesarios del OCR la hoja Facturas ############
###################################################################################################

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve`
AS
SELECT
    processed_date,
    num_factura_documento_ocr,
    dni_factura_limpio AS dni_factura_ocr,
    --emisor.ruc AS ruc_proveedor_emisor_ocr,
    razon_social_fact_limpia_ocr,
    ruc_clean AS ruc_proveedor_emisor_ocr,
    -- 2041 es 1 EPS
    --  2 es RSf
    importe.tipo_moneda AS tipo_moneda_factura_ocr,
    SAFE_CAST(subtotal_ocr_val AS FLOAT64) AS monto_sub_factura_ocr,
    SAFE_CAST(total_ocr_val AS FLOAT64) AS monto_factura_ocr,
    importe_total_tramas as monto_factura_trama,
    --SAFE_CAST(importe.monto AS FLOAT64) AS monto_factura_ocr,
    poliza_limpia AS poliza_factura_ocr,
    --info_seguro.poliza AS poliza_factura_ocr,
    ruc_compania_factura_ocr,
    CASE 
      WHEN receptor.ruc = '20414955020' THEN 1 -- RIMAC EPS
      WHEN receptor.ruc = '20100041953' THEN 2 -- RIMAC SALUD
      ELSE 0 -- Los NULL
    END AS flag_compania_factura_ocr,
    fecha_emision_fact_limpio_val,
    --fecha_emision_fact_limpio,
    info_seguro.encuentro AS encuentro_factura_ocr,
    codigo_autorizacion_limpio AS cod_autorizacion_factura_ocr,
    --paciente.codigo_autorizacion AS cod_autorizacion_factura_ocr,

    -- FLAGS
    flag_ruc_invalido,
    --flag_codigo_autorizacion_invalido,
    flag_poliza_invalida as flag_poliza_ocr_invalida,
    --flag_fact_coincide, -- select distinct info_seguro.encuentro
    flag_medicamento_factura_ocr,
  FROM `{{project_id}}.siniestro_salud_auna.auna_invoice_mvp_last_of_v1`
  --`{{project_id}}.tmp.auna_pdfs_factura_entidades`
;


-- SELECT COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve`
-- 1065
-- SELECT COUNT(DISTINCT num_factura_documento_ocr) FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve`
-- 1065
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve` LIMIT 50

-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve` WHERE num_factura_documento_ocr = 'F136-00016606'
-- SELECT DISTINCT processed_date FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve`
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve` WHERE fecha_emision_fact_limpio_val IS NULL


###################################################################################################
############## PASO 11 Opcional: Análisis de nulos en la tabla OCR Factura Breve ##################
###################################################################################################

WITH total_registros AS (
  SELECT COUNT(*) AS cantidad_total 
  FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve`
),
conteo_nulos AS (
  SELECT
    COUNT(*) - COUNT(num_factura_documento_ocr) AS num_factura_nulos,
    COUNT(*) - COUNT(dni_factura_ocr) AS dni_nulos,
    COUNT(*) - COUNT(ruc_proveedor_emisor_ocr) AS ruc_proveedor_nulos,
    COUNT(*) - COUNT(tipo_moneda_factura_ocr) AS tipo_moneda_nulos,
    COUNT(*) - COUNT(monto_sub_factura_ocr) AS monto_sub_nulos,
    COUNT(*) - COUNT(monto_factura_ocr) AS monto_total_nulos,
    COUNT(*) - COUNT(poliza_factura_ocr) AS poliza_nulos,
    COUNT(*) - COUNT(ruc_compania_factura_ocr) AS ruc_compania_nulos,
    COUNT(*) - COUNT(flag_compania_factura_ocr) AS flag_compania_nulos,
    COUNT(*) - COUNT(fecha_emision_fact_limpio_val) AS fecha_emision_nulos,
    COUNT(*) - COUNT(encuentro_factura_ocr) AS encuentro_nulos,
    COUNT(*) - COUNT(cod_autorizacion_factura_ocr) AS cod_autorizacion_nulos,
    COUNT(*) - COUNT(flag_ruc_invalido) AS flag_ruc_nulos,
    --COUNT(*) - COUNT(flag_codigo_autorizacion_invalido) AS flag_cod_aut_nulos,
    COUNT(*) - COUNT(flag_poliza_ocr_invalida) AS flag_poliza_nulos
  FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve`
)
SELECT
  'num_factura_documento_ocr' AS campo,
  t.cantidad_total,
  num_factura_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'dni_factura_ocr' AS campo,
  t.cantidad_total,
  dni_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'ruc_proveedor_emisor_ocr' AS campo,
  t.cantidad_total,
  ruc_proveedor_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'tipo_moneda_factura_ocr' AS campo,
  t.cantidad_total,
  tipo_moneda_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'monto_sub_factura_ocr' AS campo,
  t.cantidad_total,
  monto_sub_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'monto_factura_ocr' AS campo,
  t.cantidad_total,
  monto_total_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'poliza_factura_ocr' AS campo,
  t.cantidad_total,
  poliza_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'ruc_compania_factura_ocr' AS campo,
  t.cantidad_total,
  ruc_compania_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'flag_compania_factura_ocr' AS campo,
  t.cantidad_total,
  flag_compania_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'fecha_emision_fact_limpio_val' AS campo,
  t.cantidad_total,
  fecha_emision_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'encuentro_factura_ocr' AS campo,
  t.cantidad_total,
  encuentro_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'cod_autorizacion_factura_ocr' AS campo,
  t.cantidad_total,
  cod_autorizacion_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
SELECT
  'flag_ruc_invalido' AS campo,
  t.cantidad_total,
  flag_ruc_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
UNION ALL
-- SELECT
--   'flag_codigo_autorizacion_invalido' AS campo,
--   t.cantidad_total,
--   flag_cod_aut_nulos AS cantidad_nulos
-- FROM conteo_nulos, total_registros t
-- UNION ALL
SELECT
  'flag_poliza_ocr_invalida' AS campo,
  t.cantidad_total,
  flag_poliza_nulos AS cantidad_nulos
FROM conteo_nulos, total_registros t
ORDER BY cantidad_nulos DESC
;

