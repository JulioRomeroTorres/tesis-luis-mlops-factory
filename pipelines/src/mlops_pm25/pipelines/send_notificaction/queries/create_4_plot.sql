################# FUENTES USADAS ######################
-- 1. `{{project_id}}.siniestro_salud_auna.SEPS_TEMP_ATENCION_M`


#####################################################################
#### PASO 01: Crear tabla trama Carta de Garantia con ult carta #####
#####################################################################
-- select * from `{{project_id}}.siniestro_salud_auna.trama_cartagarantia_previa` WHERE primer_tipo_doc_autorizacion = '3'
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.trama_cartagarantia_previa`
AS
WITH 
base as (
  select
  RUCIPRESS as ruc_tramaCG,
  TIPODOCUMENTOIDENTIDAD tipo_de_documento_de_identidad,
  NUMERODOCUMENTOPAGO num_factura_trama_carta,
  
  -- Campos de la trama tedef CARTA GARANTIA
  TIPODOCUMENTOPRESTACION AS primer_tipo_doc_autorizacion,
  NUMEROAUTORIZACION AS numero_del_documento_de_autorizacion, -- 
  SEGUNDOTIPOAUTORIZACION AS seg_tipo_doc_autorizacion,
  SEGUNDONROAUTORIZACION AS seg_numero_del_documento_de_autorizacion, --
  ------
  
  NUMERODOCUMENTOIDENTIDAD numero_del_documento_de_identidad,
  #fecha_emision_siteds_ocr
  FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', FECHAINICIOATENCION)) fecha_de_prestacion,
  TIPOCOBERTURA,
  CODIGOPROFESIONALATENCION,
  TIPOAFILIACIONPACIENTE
  
  from `{{project_id}}.siniestro_salud_auna.SEPS_TEMP_ATENCION_M`
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY RUCIPRESS,NUMERODOCUMENTOPAGO ORDER BY CODIGOLOTE DESC) = 1
--limit 10
),
base_final AS (
  SELECT *
  FROM base
  QUALIFY
      ROW_NUMBER() OVER (PARTITION BY ruc_tramaCG,num_factura_trama_carta ORDER BY fecha_de_prestacion ASC) = 1
)
select *
from base_final
WHERE TRUE
AND primer_tipo_doc_autorizacion IN ('3', '03') OR seg_tipo_doc_autorizacion IN ('3', '03')
;

