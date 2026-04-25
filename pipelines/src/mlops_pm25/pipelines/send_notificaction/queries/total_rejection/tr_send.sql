####################################################################
########################## ENTREGABLE FINAL ########################
####################################################################


########################################################
########### 1. Crear tabla total de los casos ##########
########################################################

DECLARE PERIODO_INI STRING;


SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

-- Eliminamos los registros de esa fecha, antes del insert, por si acaso
DELETE FROM `{{project_id}}.siniestro_salud_auna.envio_auditoria_proveedores_final`
WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
;

-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.envio_auditoria_proveedores_final` WHERE processed_date = '2025-11-14'
-- Insert de esas fechas
INSERT INTO `{{project_id}}.siniestro_salud_auna.envio_auditoria_proveedores_final`
--CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.envio_auditoria_proveedores_final` AS
SELECT 
DISTINCT
  B.bandeja,
  A.* EXCEPT(grupo_clinica_texto),
  B.des_cobertura,
  CASE WHEN B.tip_caso_especial IS NULL THEN 'Flujo regular' ELSE B.tip_caso_especial END AS tip_caso_especial,
  B.proveedor,
  B.sede,
  A.grupo_clinica_texto AS red,
  B.des_producto,
  B.paciente,
  B.nom_completo_contratante,
  B.desc_estado,
  CAST(B.motivo_notificado AS STRING) AS motivo_notificado,
  CASE WHEN B.bandeja IS NOT NULL THEN 1 ELSE 0 END AS flag_ocurrenciaActual, -- SELECT distinct processed_date
FROM  `{{project_id}}.siniestro_salud_auna.base_reglas_finales_proveedores` AS A
LEFT JOIN `{{project_id}}.siniestro_salud_auna.auna_facturas_RS_oficial` AS B
ON A.num_factura_documento_ocr = B.factura AND a.num_siniestro = B.num_siniestro
WHERE TRUE
AND A.processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
AND A.reglas_mensaje <> 'La cantidad de siteds de CPM en la trama no coincide con RS'
;



#######################################################
############### 2. ACTUALIZAR EXCEL AUDITORIA #########
#######################################################

-- Eliminamos los registros de esa fecha, antes de el insert, por si acaso
DELETE FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_proveedores_final`
WHERE fecha_proceso = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
;

-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_proveedores_final` WHERE fecha_proceso = '2025-11-14'
-- Insert de esas fechas
INSERT INTO `{{project_id}}.siniestro_salud_auna.reporte_auditoria_proveedores_final`
-- CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.reporte_auditoria_proveedores_final` AS
WITH datos AS (
  SELECT
    bandeja,
    num_factura_documento_ocr AS num_factura,
    ruc_proveedor_emisor_ocr,
    processed_date AS fecha_proceso,
    num_siniestro,
    factura_duplicada,
    fec_atencion_duplicado,
    num_siniestro_factDup,
    dsc_dup,
    estado_procesado_duplicado,
    estado AS estado_alerta_IA,
    reglas_mensaje AS alerta_IA,
    monto_factura_ocr AS monto_factura,
    des_producto,
    paciente,
    nom_completo_contratante,
    desc_estado,
    motivo_notificado,
    des_cobertura,
    tip_caso_especial,
    proveedor,
    sede,
    red,
    CASE WHEN reglas_mensaje IN (
      'Contratante con autoseguro vencido',
      'Afiliado no vigente en RS entre las fechas de la carta de garantia',
      'Factura con duplicidad de pago',
      'La cantidad de siteds de CPM en la trama no coincide con RS',
      'CPM cronico con mas de una atencion en un mismo grupo de clinica'
    ) THEN 1 ELSE 0 END AS flag_reglas_ahorro,
    CURRENT_TIMESTAMP() AS time_stamp,
    num_lote,
    CAST(NULL AS STRING) AS flag_alerta_efectiva,
    ROW_NUMBER() OVER (
      PARTITION BY
        bandeja,
        num_factura_documento_ocr,
        CAST(processed_date AS STRING),  -- ✅ Convertido para evitar FLOAT64
        num_siniestro,
        factura_duplicada,
        fec_atencion_duplicado,
        num_siniestro_factDup,
        dsc_dup,
        estado_procesado_duplicado,
        estado,
        reglas_mensaje,
        des_producto,
        paciente,
        nom_completo_contratante,
        desc_estado,
        motivo_notificado,
        des_cobertura,
        tip_caso_especial,
        proveedor,
        sede,
        red
      ORDER BY CAST(num_lote AS INT64) DESC
    ) AS rn -- select distinct processed_date
  FROM `{{project_id}}.siniestro_salud_auna.envio_auditoria_proveedores_final` 
  WHERE tip_caso_especial = 'Flujo regular'
    AND processed_date = PERIODO_INI
    AND estado <> 'OK'
)
SELECT * EXCEPT(rn)
FROM datos
WHERE rn = 1
;



#######################################################
############### 3. ADJUNTAR EXCEL AUDITORIA ###########
#######################################################

# Esto es una temporal, normal que se chanque porque ya he creado la fuente líneas anteriores
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.reporte_excel_auditoria_proveedores_final`
AS
SELECT
DISTINCT
  bandeja,
  num_factura,
  num_siniestro,
  factura_duplicada,
  fec_atencion_duplicado,
  num_siniestro_factDup,
  dsc_dup,
  estado_procesado_duplicado,
  estado_alerta_IA,
  alerta_IA,
  monto_factura,
  des_cobertura,
  tip_caso_especial,
  proveedor,
  sede,
  CASE 
    WHEN red = 'AUNA' THEN 'RED AUNA' ELSE red 
  END AS red,
  des_producto,
  paciente,
  nom_completo_contratante,
  desc_estado,
  motivo_notificado,
  NULL as flag_alerta_efectiva,
  flag_reglas_ahorro,
  num_lote
FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_proveedores_final`
WHERE TRUE
AND fecha_proceso = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
AND estado_alerta_IA <> 'OK' 
--AND num_factura = 'F70100013063'
;



-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.reporte_excel_auditoria_proveedores_final` 

-- SELECT
--   fecha_proceso,
--   num_factura,
--   COUNT(DISTINCT bandeja) AS cantidad_bandejas
-- FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
-- WHERE fecha_proceso BETWEEN '2025-09-01' AND '2025-09-25'
-- GROUP BY ALL
-- HAVING COUNT(DISTINCT bandeja) > 1





#################################################################
############### 4. ACTUALIZAR TABLA DE RESUMEN CORREO ###########
#################################################################
-- Eliminamos los registros de esa fecha, antes de el insert, por si acaso
DELETE FROM `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_proveedores_final`
WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
;

INSERT INTO `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_proveedores_final`
--CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_proveedores_final` AS
with tabla_inic as (
    SELECT 
        fecha_proceso processed_date,
        num_factura num_factura_documento_ocr,
        num_siniestro num_siniestro,
        proveedor,sede,red,
        --SUM(CASE WHEN reglas_mensaje = 'Factura OK' THEN 0 ELSE 1 END) AS num_obs,
        MAX(case when estado_alerta_IA='OBSERVADA' then  num_factura end) AS factura_observado,
        MAX(case when estado_alerta_IA='OBSERVADA' then  monto_factura end) AS monto_observado,
        MAX(monto_factura) AS monto_total --monto_factura_ocr monto_factura_trama
    FROM  `{{project_id}}.siniestro_salud_auna.reporte_auditoria_proveedores_final`
    WHERE fecha_proceso = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
    GROUP BY ALL

),
tabla_potencial as 

(
select num_factura num_factura_documento_ocr,num_siniestro,monto_factura monto_factura_trama,
flag_reglas_ahorro flag_pot_ahorro,estado_alerta_IA
--select *
from  `{{project_id}}.siniestro_salud_auna.reporte_auditoria_proveedores_final` 
),
tabla_ahorro_fin as (
  select num_factura_documento_ocr,
  num_siniestro,
  count(1) num_pot,
  max(monto_factura_trama) monto_potencial,
  --max(case when estado_alerta_IA='OBSERVADA' then monto_factura_trama end) monto_observado
  from tabla_potencial
  where flag_pot_ahorro = 1
  group by all
),
tabla_final as (
  select a.*,
  b.num_factura_documento_ocr factura_potencial,
  b.num_pot,
  b.monto_potencial,
  --b.monto_observado
  from tabla_inic a
  left join tabla_ahorro_fin b
  on (a.num_factura_documento_ocr = b.num_factura_documento_ocr and a.num_siniestro = b.num_siniestro)
)
 select 
  processed_date,
  COUNT(num_factura_documento_ocr) as ctdFacturas,
  SUM(monto_total) AS mtoFacturado,
  COUNT(factura_observado) AS ctdFactObservadas,
  COUNT(factura_observado) / COUNT(num_factura_documento_ocr) AS pctFactObs, -- Esto es lo que está en paréntesis 
  SUM(monto_observado) AS mtoObservado,
  --COUNT(factura_potencial) AS ctdFactPot, 
  SUM(monto_potencial) AS mtoPotAhorro,
  SUM(monto_potencial) / SUM(monto_observado) AS pctMtoAhorrovsObservador, -- Esto es lo que está en paréntesis 
 from tabla_final
 GROUP BY ALL
 ORDER BY 1 DESC
 ;



#################################################################
############### 5. FORMATO DEL RESUMEN CORREO ENVIO #############
#################################################################
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.resumen_correo_diario_auditoria_final`

-- Esto es una temporal, normal que se chanque
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.resumen_correo_diario_auditoria_proveedores_final`
AS
SELECT
  * EXCEPT(processed_date)
FROM `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_proveedores_final`
WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
;

-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.resumen_correo_diario_auditoria_proveedores_final`