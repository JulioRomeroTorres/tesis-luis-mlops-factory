################# FUENTES USADAS ######################
-- 1. `{{project_id}}.siniestro_salud_auna.SEPS_DOCUMENTO_FACTURADOR_M`



###########################################################################
################ PASO 1: Creación de TRAMA FACTURA ########################
###########################################################################

------- CREAMOS TABLA DE TRAMA
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_rfs`
  AS

  SELECT 
  NUMERODOCUMENTOPAGO numero_de_documento_de_pago,
  FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', FECHAEMISION)) fecha_emision_tramas,
  CODIGOPRODUCTO,
  RUCIPRESS ruc_tramas,
        CASE TIPOMONEDA
        WHEN '1' THEN 'soles'
        WHEN '2' THEN 'dolares'
        ELSE CAST(TIPOMONEDA AS STRING)
      END AS tipo_moneda_tramas,
 
      SAFE_CAST(MONTOTOTAL AS FLOAT64) AS importe_total_tramas,
      SAFE_CAST(MONTOIGV AS FLOAT64) AS igv_tramas,
 
      SAFE_CAST(MONTONETO AS FLOAT64) AS subtotal_tramas,
      CAST(CODIGOMECANISMOPAGO AS STRING) AS descripcion_tramas,
 
#  MONTONETO, #SAFE_CAST(t1.base_imponible AS FLOAT64) AS subtotal_tramas, validar con Jose
 
  FROM `{{project_id}}.siniestro_salud_auna.SEPS_DOCUMENTO_FACTURADOR_M`
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY RUCIPRESS,NUMERODOCUMENTOPAGO ORDER BY CODIGOLOTE DESC) = 1
;

