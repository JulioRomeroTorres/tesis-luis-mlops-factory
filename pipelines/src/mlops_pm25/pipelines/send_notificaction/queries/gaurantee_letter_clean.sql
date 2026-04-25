################# FUENTES USADAS ######################
-- 1. `{{project_id}}.genai_documents.auna_documents`
-- 2. `{{project_id}}.genai_documents.auna_guarantee_letter_mvp`


DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

##############################################################
######### PASO 01: Creación Tabla CARTA GARANTIA OCR  ########
##############################################################

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample`
AS
WITH source_file as (
  SELECT *,
    invoice_number as num_factura_documento_ocr,
    --REPLACE(REGEXP_EXTRACT(file_path, r'F\d+_\d+'), "_", "-") as  num_factura_documento_ocr,
    REGEXP_EXTRACT(file_name, r'^(\d{11})') AS ruc_emisor_path,
  FROM `{{project_id}}.genai_documents.auna_documents`
  -- ESTO SERÁ TEMPORAL
  --WHERE processed_date = '2025-07-09'
  WHERE processed_date = PERIODO_INI --'2025-07-09' AND '2025-07-19' 
  QUALIFY
      ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY processed_date DESC,cutt_off_date DESC) = 1
-- where created_at > '2025-06-25 00:00:0.00 UTC'
)
select
  -- processed_date,
  cg.*,
  num_factura_documento_ocr,
  ruc_emisor_path,
from  source_file  
inner join `{{project_id}}.genai_documents.auna_guarantee_letter_mvp` as cg 
on  source_file.id = cg.documento_id
;

-- SELECT processed_date, COUNT(DISTINCT num_factura_documento_ocr) FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample` GROUP BY ALL ORDER BY 1 DESC

####################################################################################################
############################## PASO 2.1: Limpiar Fecha CG ##########################################
####################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` AS
SELECT 
  *,
  CASE 
    WHEN REGEXP_REPLACE(encabezado.fec_emision, r'[^0-9/]', '') IN ('', 'null') THEN NULL
    ELSE SAFE.PARSE_DATE('%d/%m/%Y', REGEXP_REPLACE(encabezado.fec_emision, r'[^0-9/]', ''))
  END AS fec_emision_clean,

  CASE 
    WHEN REGEXP_REPLACE(encabezado.fec_val_sol, r'[^0-9/]', '') IN ('', 'null') THEN NULL
    ELSE SAFE.PARSE_DATE('%d/%m/%Y', REGEXP_REPLACE(encabezado.fec_val_sol, r'[^0-9/]', ''))
  END AS fec_val_sol_clean
FROM  `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample`
;


####################################################################################################
############################## PASO 2.2: Limpiar Monto Total CG ####################################
####################################################################################################
-- Limpiar limites_garantizados.monto_total (ejemplo: 'S/. 10,000.00')
-- Quiero crear un campo monto_total_cg_clean y que elimine espacios en blanco, 'S/', 'comas', es decir, que sea solo el número con sus decimales (ejemplo S/. 7,763.86 que sea 7763.86)


CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_v1` AS
SELECT
    *,
    
    -- Campo: monto_total_cg_clean
    CASE
        WHEN REGEXP_CONTAINS(LOWER(IFNULL(t.limites_garantizados.monto_total, '')), r'(%|tope|saldo)') THEN NULL
        ELSE SAFE_CAST(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(IFNULL(t.limites_garantizados.monto_total, ''), 'S/. ', ''),
                                        'S/.', ''
                                    ),
                                    's/. ', ''
                                ),
                                'S/', ''
                            ),
                            's/', ''
                        ),
                        ',', ''
                    ),
                    r'\d+(?:\.\d+)?' -- solo un grupo de captura implícito
                ),
                r'[^\d.]', ''
            ) AS NUMERIC
        )
    END AS monto_total_cg_clean

FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` AS t
;


/*
Explicación de la limpieza:
1.  t.limites_garantizados.monto_total: Accede al valor original. Asumo que `limites_garantizados` es un STRUCT y `monto_total` es un campo dentro de él. Si `limites_garantizados.monto_total` es el nombre completo de la columna, simplemente úsalo.
2.  REPLACE(..., 'S/.', ''): Remueve la cadena 'S/.' del inicio. Se asume que el 'S/.' siempre está sin espacio entre 'S/.' y el número. Si pudiera ser 'S/. ' con un espacio, considera usar 'S/. ' en el REPLACE.
3.  REPLACE(..., ',', ''): Remueve todas las comas, que se usan como separadores de miles.
4.  TRIM(...): Elimina cualquier espacio en blanco que pudiera quedar al principio o al final de la cadena después de las operaciones REPLACE. Esto es útil para asegurar una conversión limpia.
5.  CAST(... AS NUMERIC): Convierte la cadena resultante (por ejemplo, '7763.86') a un tipo de dato NUMERIC. NUMERIC mantiene la precisión de los números decimales, lo cual es ideal para cálculos financieros. Si necesitas el rendimiento de coma flotante y la precisión no es crítica, podrías usar `FLOAT64`.

Ejemplos de transformación:
- 'S/. 10,000.00'  ->  '10,000.00' (después de 1er REPLACE) -> '10000.00' (después de 2do REPLACE) -> '10000.00' (después de TRIM) -> 10000.00 (después de CAST AS NUMERIC)
- 'S/. 7,763.86'   ->  '7,763.86'  (después de 1er REPLACE) -> '7763.86'  (después de 2do REPLACE) -> '7763.86'  (después de TRIM) -> 7763.86 (después de CAST AS NUMERIC)

*/

---------- Validacion Monto Total
-- SELECT DISTINCT limites_garantizados. monto_total FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean`
-- SELECT COUNT(DISTINCT num_factura_documento_ocr) FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` WHERE limites_garantizados. monto_total IS NULL
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` WHERE limites_garantizados. monto_total IS NULL
-- SELECT DISTINCT monto_total_cg_clean FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean`
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` WHERE num_factura_documento_ocr = 'F105-00008875' --(S/. 13,806.00)


####################################################################################################
########################## PASO 2.3: Limpiar Monto Acumulado CG ####################################
####################################################################################################
-- Limpiar limites_garantizados.monto_acumulado
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_v2` AS
SELECT
    *,
-- Campo: monto_acumulado_cg_clean
    CASE
        WHEN REGEXP_CONTAINS(LOWER(IFNULL(t.limites_garantizados.monto_acumulado, '')), r'(%|tope|saldo)') THEN NULL
        ELSE SAFE_CAST(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(IFNULL(t.limites_garantizados.monto_acumulado, ''), 'S/. ', ''),
                                        'S/.', ''
                                    ),
                                    's/. ', ''
                                ),
                                'S/', ''
                            ),
                            's/', ''
                        ),
                        ',', ''
                    ),
                    r'\d+(?:\.\d+)?'
                ),
                r'[^\d.]', ''
            ) AS NUMERIC
        )
    END AS monto_acumulado_cg_clean

FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_v1` AS t
;





/*
Explicación de la CORRECCIÓN:
- `SELECT *`: Esto asegura que todas las columnas de la tabla `auna_cartag_mvp_sample_clean` (incluyendo `monto_total_cg_clean` que ya fue creada y limpiada en el paso anterior) se seleccionen.
- Solo se añade la definición de la *nueva* columna `monto_acumulado_cg_clean`. No es necesario recalcular `monto_total_cg_clean`.

*/

---------- Validacion Monto Acumulado
-- SELECT DISTINCT limites_garantizados. monto_total FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean`
-- SELECT COUNT(DISTINCT num_factura_documento_ocr) FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` WHERE limites_garantizados. monto_acumulado IS NULL
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` WHERE limites_garantizados. monto_acumulado IS NULL

-- SELECT DISTINCT num_factura_documento_ocr, limites_garantizados. monto_acumulado, monto_acumulado_cg_clean FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` where limites_garantizados. monto_acumulado like '%SE EMITE%'






####################################################################################################
########################## PASO 2.4: Razon Social - Limpieza #######################################
####################################################################################################
-- SELECT DISTINCT encabezado.proveedor_nombre FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_unico` WHERE encabezado.proveedor_nombre LIKE '%ONCOCENTER%'

# Quiero limpiar el campo encabezado.proveedor_nombre y crear uno nuevo en llamado razon_social_proveedor_cg_limpia_ocr. Para hacerlo, primero eliminar ('.', ',') en los valores. Luego, lo que diga textualmente 'null' o valor vacío ('') asignarle null. Por último, aplicar los case when de abajo. 
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_v3` AS
SELECT
  *,
  CASE
    WHEN TRIM(REPLACE(REPLACE(encabezado.proveedor_nombre, '.', ''), ',', '')) IN ('', 'null') THEN NULL
    WHEN TRIM(REPLACE(REPLACE(encabezado.proveedor_nombre, '.', ''), ',', '')) LIKE 'CLINICA DELGADO' THEN 'MEDIC SER SAC'
    WHEN TRIM(REPLACE(REPLACE(encabezado.proveedor_nombre, '.', ''), ',', '')) LIKE '%ONCOCENTER%' THEN 'ONCOCENTER PERU SAC'
    ELSE TRIM(REPLACE(REPLACE(encabezado.proveedor_nombre, '.', ''), ',', ''))
  END AS razon_social_proveedor_cg_limpia_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_v2`
;


--- VALIDACION RAZON SOCIAL
-- SELECT DISTINCT razon_social_proveedor_cg_limpia_ocr FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean`




####################################################################################################
############################ PASO 2.5: RUC - Limpieza ##############################################
####################################################################################################
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_v4` AS

SELECT
  *,
  -- ① primera coincidencia de 11 dígitos  →  STRING
  --REGEXP_EXTRACT(emisor.ruc, r'(\d{11})')     AS ruc_clean,
  REGEXP_EXTRACT(encabezado.proveedor_ruc, r'(\d{11})') AS ruc_proveedor_cg_ocr,
  
  -- opcional: bandera para saber si NO se halló un RUC válido
  --REGEXP_EXTRACT(emisor.ruc, r'(\d{11})') IS NULL  AS flag_ruc_invalido
  REGEXP_EXTRACT(encabezado.proveedor_ruc, r'(\d{11})') IS NULL  AS flag_ruc_cg_invalido

FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_v3`
;

-------- VALIDACION RUC
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` WHERE ruc_proveedor_cg_ocr IS NULL






###################################################################################################
################# PASO 04 (FINAL): Quedarnos con cartas únicas en CG OCR ########################
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_unico`
AS WITH
tabla_ini AS (
select
*

FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_v4`
 
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY num_factura_documento_ocr ORDER BY
      monto_acumulado_cg_clean IS NULL,monto_acumulado_cg_clean DESC) = 1
)

FROM tabla_ini
;



------------ VALIDACION FACTURAS DUPLICADAS (debería salir tabla vacía)
WITH facturas_duplicadas AS (
  SELECT 
    num_factura_documento_ocr,
    COUNT(*) as conteo
  --FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean`
  FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_unico`
  GROUP BY num_factura_documento_ocr
  HAVING COUNT(*) > 1
  --ORDER BY 2 DESC
)
SELECT t.*
--FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` t
FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_unico` t
JOIN facturas_duplicadas fd ON t.num_factura_documento_ocr = fd.num_factura_documento_ocr
ORDER BY t.num_factura_documento_ocr -- puedes ordenar por otros campos relevantes
;




####################################################################################################
########################## PASO 5: Crear campo de monto final CG ###################################
####################################################################################################

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_unico_v1` 
AS
SELECT
    *,
    CASE 
        WHEN monto_acumulado_cg_clean IS NULL OR monto_acumulado_cg_clean = 0 OR (monto_acumulado_cg_clean < monto_total_cg_clean) THEN monto_total_cg_clean 
        ELSE monto_acumulado_cg_clean 
    END AS monto_final_cg
    --monto_acumulado_cg_clean AS monto_final_cg,
    --CASE WHEN monto_total_cg_clean IS NULL THEN monto_acumulado_cg_clean END AS monto_final_cg
FROM
    `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_unico` AS t
;


####################################################################################################
####################### PASO 06: Crear campo DX Entrada Epicrisis ##################################
####################################################################################################
-- La idea es crear un campo de dx de entrada de Carta de Garantía para luego compararlo con el de epicrisis

-- PASO 2.6: Crear campo DX Entrada Epicrisis (robusto con normalización)
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_unico_v2` AS
WITH base AS (
  SELECT
    t.*,
    -- Normalización de entrada: mayúsculas, sin tildes, espacios compactados
    UPPER(
      REGEXP_REPLACE(
        TRANSLATE(
          REGEXP_REPLACE(
            TRIM(TRANSLATE(informe_medico.diagnostico, 'ÁÉÍÓÚÜÑáéíóúüñ', 'AEIOUUNAEIOUUN')),
            r'\s+',
            ' '
          ),
          'ΑΒΕΖΗΙΚΜΝΟΡΤΥΧ',  -- Letras griegas visualmente similares
          'ABEZHIKMNOPTYX'   -- Equivalentes latinos
        ),
        r'\s+',
        ' '
      )
    ) AS dx_norm

  FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_unico_v1` AS t
)
SELECT
  base.*,
  CASE
    WHEN dx_norm = 'TUMOR MALIGNO DE LA PROSTATA' THEN 'HIPERPLASIA DE LA PROSTATA'
    WHEN dx_norm = 'OTROS TRASTORNOS DE LOS MENISCOS' THEN 'DESGARRO DE MENISCOS, PRESENTE'
    WHEN dx_norm = 'TRASTORNO DE MENISCO DEBIDO A DESGARRO O LESION ANTIGUA' THEN 'DESGARRO DE MENISCOS, PRESENTE'
    WHEN dx_norm = 'DESGARRO DE MENISCOS, PRESENTE' THEN 'DESGARRO DE MENISCOS, PRESENTE'
    WHEN dx_norm = 'INSUFICIENCIA RENAL CRONICA, NO ESPECIFICADA' THEN 'Insuficiencia Renal Crónica. (antecedente)'
    WHEN dx_norm = 'ANGINA DE PECHO CON ESPASMO DOCUMENTADO' THEN 'Cardiopatía Coronaria Isquémica (antecedente)'
    WHEN dx_norm = 'ENFERMEDAD ATEROSCLEROTICA DEL CORAZON' THEN 'Cardiopatía Coronaria Isquémica (antecedente)'
    WHEN dx_norm = 'TUMOR MALIGNO DE LA GLANDULA TIROIDES' THEN 'TUMOR MALIGNO DE LA GLANDULA TIROIDES'
    WHEN dx_norm = 'APENDICITIS AGUDA CON ABSCESO PERITONEAL' THEN 'APENDICITIS AGUDA CON PERITONITIS GENERALIZADA'
    WHEN dx_norm = 'HIPERPLASIA DE LA PROSTATA' THEN 'HIPERPLASIA DE LA PROSTATA'
    WHEN dx_norm = 'OTROS DOLORES ABDOMINALES Y LOS NO ESPECIFICADOS' THEN 'DOLORES ABDOMINALES Y LOS NO ESPECIFICADOS'
    WHEN dx_norm = 'COMPRESIONES DE LAS RAICES Y PLEXOS NERVIOSOS EN LA ESPONDILOSIS (M47.-+)' THEN 'COMPRESIONES DE LAS RAICES Y PLEXOS NERVIOSOS EN TRASTORNOS DE LOS DISCOS INTERVERTEBRALES (M50-M51'
    WHEN dx_norm = 'FIBRILACION Y ALETEO AURICULAR' THEN 'Fibrilación Atrial Paroxística (antecedente)'
    WHEN dx_norm = 'ENFERMEDAD CARDIOVASCULAR ATEROSCLEROTICA, ASI DESCRITA' THEN 'Cardiopatía Coronaria Isquémica (antecedente)'
    WHEN dx_norm = 'CALCULO DE CONDUCTO BILIAR SIN COLANGITIS NI COLECISTITIS' THEN 'CALCULO DE LA VESICULA BILIAR SIN COLELITIASIS'
    WHEN dx_norm = 'INFARTO AGUDO DEL MIOCARDIO, SIN OTRA ESPECIFICACION' THEN 'Cardiopatía Coronaria Isquémica (antecedente)'
    WHEN dx_norm = 'LEIOMIOMA DEL UTERO, SIN OTRA ESPECIFICACION' THEN 'LEIOMIOMA DEL UTERO, SIN OTRA ESPECIFICACION'
    WHEN dx_norm = 'PARTO POR CESAREA, SIN OTRA ESPECIFICACION' THEN 'CP. CESAREA (CUALQUIER TIPO)'
    WHEN dx_norm = 'ATENCION MATERNA POR CICATRIZ UTERINA DEBIDA A CIRUGIA PREVIA' THEN 'ATENCION MATERNA POR CICATRIZ UTERINA DEBIDA A CIRUGIA PREVIA'
    WHEN dx_norm = 'PANCREATITIS AGUDA' THEN 'PANCREATITIS AGUDA'
    WHEN dx_norm = 'HEMORRAGIA INTRAENCEFALICA, NO ESPECIFICADA' THEN 'HEMORRAGIA INTRAENCEFALICA, NO ESPECIFICADA'
    WHEN dx_norm = 'PARTO UNICO ESPONTANEO, SIN OTRA ESPECIFICACION' THEN 'PARTO UNICO ESPONTANEO, SIN OTRA ESPECIFICACION'
    WHEN dx_norm = 'PARTO UNICO ESPONTANEO, PRESENTACION CEFALICA DE VERTICE' THEN 'PARTO UNICO ESPONTANEO, PRESENTACION CEFALICA DE VERTICE'
    WHEN dx_norm = 'CARDIOMIOPATIA ISQUEMICA' THEN 'Cardiopatía Coronaria Isquémica (antecedente)'
    WHEN dx_norm = 'DESVIACION DEL TABIQUE NASAL' THEN 'Desviación de tabique nasal'
    WHEN dx_norm = 'PARTO VAGINAL EUTOSICO O DISTOSICO -C/S EPISIO C/S TRAQUELO, PARTO UNICO ESPONTANEO, SIN OTRA ESPECIFICACION' THEN 'PARTO UNICO ESPONTANEO, SIN OTRA ESPECIFICACION'
    WHEN dx_norm = 'OTRAS FRACTURAS DE OTROS DEDOS DE LA MANO' THEN 'FRACTURA DE OTRO DEDO DE LA MANO'
    WHEN dx_norm = 'PARTO VAGINAL EUTOSICO O DISTOSICO -C/S EPISIO C/S TRAQUELO, PARTO UNICO ESPONTANEO, PRESENTACION CEFALICA DE VERTICE' THEN 'PARTO UNICO ESPONTANEO, PRESENTACION CEFALICA DE VERTICE'
    WHEN dx_norm = 'FIBRILACION AURICULAR' THEN 'Fibrilación Atrial Paroxística (antecedente)'
    WHEN dx_norm = 'FRACTURA DE OTRO DEDO DE LA MANO' THEN 'FRACTURA DE OTRO DEDO DE LA MANO'
    WHEN dx_norm = 'DOLOR LOCALIZADO EN OTRAS PARTES INFERIORES DEL ABDOMEN' THEN 'DOLORES ABDOMINALES Y LOS NO ESPECIFICADOS'
    WHEN dx_norm = 'RUPTURA TRAUMATICA DE LIGAMENTOS DE LA MUÑECA Y DEL CARPO' THEN 'RUPTURA TRAUMATICA DE LIGAMENTOS DE LA MUÑECA Y DEL CARPO'
    WHEN dx_norm = 'CALCULO DE LA VESICULA BILIAR CON COLECISTITIS AGUDA' THEN 'CALCULO DE LA VESICULA BILIAR CON COLECISTITIS AGUDA'
    WHEN dx_norm = 'CALCULO DE LA VESICULA BILIAR CON OTRA COLECISTITIS' THEN 'CALCULO DE LA VESICULA BILIAR CON OTRA COLECISTITIS'
    WHEN dx_norm = 'CP. CESAREA (CUALQUIER TIPO)' THEN 'CP. CESAREA (CUALQUIER TIPO)'
    WHEN dx_norm = 'RIGIDEZ ARTICULAR, NO CLASIFICADA EN OTRA PARTE' THEN 'RIGIDEZ ARTICULAR, NO CLASIFICADA EN OTRA PARTE'
    WHEN dx_norm = 'ISQUEMIA CEREBRAL TRANSITORIA, SIN OTRA ESPECIFICACION' THEN 'ISQUEMIA CEREBRAL TRANSITORIA, SIN OTRA ESPECIFICACION'
    WHEN dx_norm = 'TUMOR MALIGNO DE GLANDULA SALIVAL MAYOR, NO ESPECIFICADA' THEN 'TUMOR MALIGNO DE GLÁNDULA SALIVAL MAYOR, NO ESPECIFICADA'
    WHEN dx_norm = 'FRACTURA DE LA EPIFISIS SUPERIOR DE LA TIBIA' THEN 'FRACTURA DE LA EPIFISIS SUPERIOR DE LA TIBIA'
    WHEN dx_norm = 'FIEBRE DEL DENGUE' THEN 'FIEBRE DEL DENGUE [DENGUE CLASICO]'
    WHEN dx_norm = 'APENDICITIS AGUDA, NO ESPECIFICADA' THEN 'APENDICITIS AGUDA, NO ESPECIFICADA'
    WHEN dx_norm = 'COMPRESIONES DE LAS RAICES Y PLEXOS NERVIOSOS EN TRASTORNOS DE LOS DISCOS INTERVERTEBRALES (M50-M51' THEN 'COMPRESIONES DE LAS RAICES Y PLEXOS NERVIOSOS EN TRASTORNOS DE LOS DISCOS INTERVERTEBRALES (M50-M51'
    WHEN dx_norm = 'LINFOMA DE CELULAS B, SIN OTRA ESPECIFICACION' THEN 'LINFOMA DE CELULAS B, SIN OTRA ESPECIFICACION'
    WHEN dx_norm = 'CELULITIS DE OTRAS PARTES DE LOS MIEMBROS' THEN 'CELULITIS DE SITIO NO ESPECIFICADO'
    WHEN dx_norm = 'ADHERENCIAS PERITONEALES' THEN 'ADHERENCIAS [BRIDAS] INTESTINALES CON OBSTRUCCION'
    WHEN dx_norm = 'COLECISTITIS AGUDA' THEN 'COLECISTITIS AGUDA'
    WHEN dx_norm = 'MIGRANA COMPLICADA' THEN 'Migraña (antecedente)'             -- ojo: MIGRAÑA -> MIGRANA en la normalización
    WHEN dx_norm = 'LEIOMIOMA INTRAMURAL DEL UTERO' THEN 'LEIOMIOMA DEL UTERO, SIN OTRA ESPECIFICACION'
    WHEN dx_norm = 'TUMOR MALIGNO DEL HIGADO, NO ESPECIFICADO' THEN 'TUMOR MALIGNO SECUNDARIO DEL HIGADO'
    WHEN dx_norm = 'FIEBRE DEL DENGUE [DENGUE CLASICO]' THEN 'FIEBRE DEL DENGUE [DENGUE CLASICO]'
    WHEN dx_norm = 'CELULITIS DE SITIO NO ESPECIFICADO' THEN 'CELULITIS DE SITIO NO ESPECIFICADO'
    WHEN dx_norm = 'ADHERENCIAS [BRIDAS] INTESTINALES CON OBSTRUCCION' THEN 'ADHERENCIAS [BRIDAS] INTESTINALES CON OBSTRUCCION'
    ELSE informe_medico.diagnostico
  END AS dx_entrada_transformada_cg_ocr
FROM base
;

-- (Opcional) reemplaza la tabla original
-- ALTER TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean` RENAME TO `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_backup_20250901`;
-- ALTER TABLE `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean_tmp` RENAME TO `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_clean`;



###################################################################################################
########### PASO 06: Crear tabla de campos necesarios del OCR de la hoja Carta Garantia ###########
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`
AS
SELECT
  documento_id,
  page_path,
  created_at,
  encabezado.titulo,
  ruc_proveedor_cg_ocr,
  razon_social_proveedor_cg_limpia_ocr,
  encabezado.compania,
  encabezado.nro_carta,
  fec_emision_clean as fec_emision,
  fec_val_sol_clean as fec_val_sol,
  monto_total_cg_clean,
  monto_acumulado_cg_clean,
  monto_final_cg,
  dx_entrada_transformada_cg_ocr,
  num_factura_documento_ocr as num_factura_cartagarantia_ocr,
  ruc_emisor_path as ruc_cg_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_cartag_mvp_sample_unico_v2`
;





###################################################################################################
####################### PASO 07 (OPCIONAL): Analisis de nulos en CG OCR ###########################
###################################################################################################

SELECT 
  'documento_id' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(documento_id) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'page_path' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(page_path) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'created_at' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(created_at) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'titulo' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(titulo) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'ruc_proveedor_cg_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(ruc_proveedor_cg_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'razon_social_proveedor_cg_limpia_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(razon_social_proveedor_cg_limpia_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'compania' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(compania) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'nro_carta' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(nro_carta) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'fec_emision' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(fec_emision) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'fec_val_sol' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(fec_val_sol) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'monto_total_cg_clean' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(monto_total_cg_clean) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'monto_acumulado_cg_clean' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(monto_acumulado_cg_clean) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'monto_final_cg' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(monto_final_cg) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'num_factura_cartagarantia_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(num_factura_cartagarantia_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

UNION ALL SELECT 
  'ruc_cg_ocr' AS campo,
  COUNT(*) AS cantidad_total,
  COUNT(*) - COUNT(ruc_cg_ocr) AS cantidad_nulos
FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`

ORDER BY cantidad_nulos DESC;

