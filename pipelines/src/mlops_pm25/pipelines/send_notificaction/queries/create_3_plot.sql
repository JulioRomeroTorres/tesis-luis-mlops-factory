################# FUENTES USADAS ######################
-- 1. `{{project_id}}.siniestro_salud_auna.SEPS_DOCUMENTO_FACTURADOR_M`


#####################################################################
####### PASO 01: Crear tabla trama Nota Credito con ult credito #####
#####################################################################
-- select distinct num_factura_trama_sited from `{{project_id}}.siniestro_salud_auna.trama_notacredito_previa`
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_notacredito_previa`
AS
SELECT 
  NUMERODOCUMENTOPAGO numero_de_documento_de_pago,

  TIPONOTA,
  NUMERONOTA,
  MONTONOTA, -- Este es el subtotal al parecer
  -- Conversión de string 'yyyymmdd' a DATE 'yyyy-mm-dd'
  PARSE_DATE('%Y%m%d', FECHANOTA) AS FECHANOTA,
  MOTIVONOTA,
  FECHAPRIMERENVIO,
  RUCIPRESS as ruc_tramaNC,
#  MONTONETO, #SAFE_CAST(t1.base_imponible AS FLOAT64) AS subtotal_tramas, validar con Jose
  FROM `{{project_id}}.siniestro_salud_auna.SEPS_DOCUMENTO_FACTURADOR_M`
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY RUCIPRESS,NUMERODOCUMENTOPAGO ORDER BY CODIGOLOTE DESC) = 1
;
