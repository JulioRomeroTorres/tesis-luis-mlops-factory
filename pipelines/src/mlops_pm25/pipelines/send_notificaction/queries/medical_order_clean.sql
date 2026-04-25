####################################
######### Fuentes Usadas ###########
####################################
-- 1. `{{project_id}}.genai_documents.auna_documents`
-- 2. `{{project_id}}.genai_documents.auna_prescription_mvp`

######################################



##############################################################
########### PASO 01: Creación Tabla RECETA OCR ###############
##############################################################

DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_receta_mvp_sample`
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
  rc.*,
  num_factura_documento_ocr,
  ruc_emisor_path,
from  source_file  -- solo hay 5 documentos para la fecha 14 corte 16
inner join `{{project_id}}.genai_documents.auna_prescription_mvp` as rc 
on  source_file.id = rc.documento_id
;


-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.auna_receta_mvp_sample` WHERE num_factura_documento_ocr = 'F119-00003529'


-- SELECT DISTINCT num_factura_documento_ocr FROM `{{project_id}}.siniestro_salud_auna.auna_receta_mvp_sample` 

-- num_factura_documento_ocr, ruc_emisor_path, medicamentos. nomb_generico, medicamentos. dosis, medicamentos. frec


###################################################################################################
############## PASO 02: Crear tabla de campos necesarios del OCR de la hoja Receta ################
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.ocr_data_receta`
AS
SELECT
  DISTINCT 
  num_factura_documento_ocr as num_factura_rc_ocr,
  ruc_emisor_path as ruc_rc_ocr, -- select *
FROM `{{project_id}}.siniestro_salud_auna.auna_receta_mvp_sample`
WHERE TRUE
;

