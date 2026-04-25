################# FUENTES USADAS ######################
-- 1. `{{project_id}}.siniestro_salud_auna.SEPS_TEMP_ATENCION_M`


##############################################################
####### PASO 01: Crear tabla trama siteds con ult siteds #####
##############################################################
-- select * from `{{project_id}}.siniestro_salud_auna.trama_sited_previa`
CREATE OR REPLACE TABLE
  `{{project_id}}.siniestro_salud_auna.trama_sited_previa`
as
with
TRAMA_SITED as (
select
RUCIPRESS,
TIPODOCUMENTOIDENTIDAD tipo_de_documento_de_identidad,
NUMERODOCUMENTOPAGO num_factura_trama_sited,
NUMEROAUTORIZACION numero_del_documento_de_autorizacion,
NUMERODOCUMENTOIDENTIDAD numero_del_documento_de_identidad,
#fecha_emision_siteds_ocr
FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', FECHAINICIOATENCION)) fecha_de_prestacion,
TIPOCOBERTURA,
CODIGOPROFESIONALATENCION,
TIPOAFILIACIONPACIENTE,
CODIGOCIE101,
CODIGOCIE102,
CODIGOCIE103,
 
from `{{project_id}}.siniestro_salud_auna.SEPS_TEMP_ATENCION_M`
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY RUCIPRESS,NUMERODOCUMENTOPAGO ORDER BY CODIGOLOTE DESC) = 1
 
 
--limit 10
),
TRAMA_SITED_FINAL AS (
  SELECT *
  FROM TRAMA_SITED
  QUALIFY
      ROW_NUMBER() OVER (PARTITION BY RUCIPRESS,num_factura_trama_sited ORDER BY fecha_de_prestacion ASC) = 1
     
)
select *
from trama_sited_final
;
