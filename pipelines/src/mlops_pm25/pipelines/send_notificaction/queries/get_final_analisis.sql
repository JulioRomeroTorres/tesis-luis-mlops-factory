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

-- Eliminamos los registros de esa fecha, antes de el insert, por si acaso
DELETE FROM `{{project_id}}.siniestro_salud_auna.envio_auditoria_final`
WHERE processed_date = PERIODO_INI; -- ACTUALIZAR FECHA O PONER EL MAX processed_date;

-- Insert de esas fechas
INSERT INTO `{{project_id}}.siniestro_salud_auna.envio_auditoria_final`
SELECT
  DISTINCT
  B.bandeja,
  A.*,
  B.des_cobertura,
  CASE WHEN B.tip_caso_especial IS NULL THEN 'Flujo regular' ELSE B.tip_caso_especial END AS tip_caso_especial,
  B.proveedor,
  B.sede,
  B.red,
  B.des_producto,
  B.paciente,
  B.nom_completo_contratante,
  B.desc_estado,
  CAST(B.motivo_notificado AS STRING) AS motivo_notificado,
  CASE WHEN B.bandeja IS NOT NULL THEN 1 ELSE 0 END AS flag_ocurrenciaActual, -- SELECT * 
  C.batch_name as num_lote
FROM  `{{project_id}}.siniestro_salud_auna.base_reglas_finales` AS A
LEFT JOIN `{{project_id}}.siniestro_salud_auna.auna_facturas_RS_oficial` AS B
ON A.num_factura_documento_ocr = B.factura AND a.num_siniestro = B.num_siniestro
LEFT JOIN (
  SELECT 
    invoice_number,
    batch_name,
    processed_date,
    ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY processed_date DESC) AS rn
  FROM `{{project_id}}.genai_documents.auna_documents`
)AS C
ON A.num_factura_documento_ocr = REPLACE(C.invoice_number, "-", "") --AND A.REGEXP_EXTRACT(B.file_name, r'^(\d{11})') -- No me traje el ruc en el excel
WHERE TRUE
AND A.processed_date = PERIODO_INI; -- ACTUALIZAR FECHA O PONER EL MAX processed_date


-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final` WHERE fecha_proceso <= '2025-09-04' AND alerta_IA = 'Factura no tiene hoja SITEDS'
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final` WHERE fecha_proceso <= '2025-09-03' AND estado_alerta_IA = 'OBSERVADA'


#######################################################
############### 2. ACTUALIZAR EXCEL AUDITORIA #########
#######################################################

-- Eliminamos los registros de esa fecha, antes de el insert, por si acaso
DELETE FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
WHERE fecha_proceso = PERIODO_INI; -- ACTUALIZAR FECHA O PONER EL MAX processed_date

-- SELECT fecha_proceso, COUNT(DISTINCT num_factura) FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final` group by all order by 1 desc

-- Insert de esas fechas
INSERT INTO `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
WITH primera_parte AS (
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
    monto_factura_trama AS monto_factura,
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
    CASE
      WHEN reglas_mensaje IN (
        'Sin Carta Garantia en la trama pero si en el sustento',
        'Sin Nota Credito en la trama pero si en el sustento', 
        'La fecha de emision de NC y Trama no coinciden',
        'El codigo de autorizacion de la hoja siteds no coincide con el de Liquidacion'
      ) THEN 'SI' ELSE 'NO'
    END AS flag_alerta_efectiva
  FROM `{{project_id}}.siniestro_salud_auna.envio_auditoria_final` AS A
  WHERE A.tip_caso_especial = 'Flujo regular'
    AND A.processed_date = PERIODO_INI -- Poner la misma fecha del processed_date
),
segunda_parte AS (
  SELECT *
  FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_proveedores_final`
  WHERE tip_caso_especial = 'Flujo regular'
    AND fecha_proceso = PERIODO_INI -- Poner la misma fecha del processed_date
)
SELECT * FROM primera_parte
UNION ALL
SELECT * FROM segunda_parte; -- ACTUALIZAR FECHA O PONER EL MAX processed_date



####################################################################
################### 2.1 ACTUALIZAR TABLA CON RED ###################
####################################################################
UPDATE `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
SET red = 'RED AUNA'
WHERE red IS NULL
  AND ruc_proveedor_emisor_ocr IN (
    '20546292658',
    '20501781291',
    '20454135432',
    '20394674371',
    '20381170412',
    '20102756364',
    '20100251176'
);




####################################################################
############### 2.2 ELIMINAR DUPLICADOS DE EXCEL AUDITORIA #########
####################################################################
-- Quiero actualizar la tabla y en caso un mismo num_factura tenga mas de un registro, eliminar de la tabla solamente el registro que diga estado_alerta_IA = 'OK' de la misma num_factura. O sea un num_factura ('1567') que tenga dos registros, uno con estado_alerta_IA 'OK' y otro con estado_alerta_IA 'OBSERVADA', eliminar el que diga 'OK'
DELETE FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
WHERE estado_alerta_IA = 'OK'
AND num_factura IN (
  SELECT num_factura
  FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
  WHERE fecha_proceso = PERIODO_INI
  GROUP BY num_factura
  HAVING COUNT(*) > 1
)
AND fecha_proceso = PERIODO_INI
;



#######################################################
############### 3. ADJUNTAR EXCEL AUDITORIA ###########
#######################################################



# Esto es una temporal, normal que se chanque porque ya he creado la fuente líneas anteriores
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.reporte_excel_auditoria_final`
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
  red,
  des_producto,
  paciente,
  nom_completo_contratante,
  desc_estado,
  motivo_notificado,
  flag_alerta_efectiva,
  CASE 
    WHEN flag_reglas_ahorro = 1 AND estado_alerta_IA <> 'OK' THEN 'Alerta rechazo' 
    WHEN estado_alerta_IA = 'OK' THEN 'No alerta'
    ELSE 'Otras alertas' 
  END AS flag_alerta_rechazo,
  num_lote
FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
WHERE TRUE
AND fecha_proceso = PERIODO_INI; -- ACTUALIZAR FECHA O PONER EL MAX processed_date




-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_final` ORDER BY 1 DESC
#################################################################
############### 4. ACTUALIZAR TABLA DE RESUMEN CORREO ###########
#################################################################
-- Eliminamos los registros de esa fecha, antes de el insert, por si acaso
DELETE FROM `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_final`
WHERE processed_date = PERIODO_INI; -- ACTUALIZAR FECHA O PONER EL MAX processed_date


INSERT INTO `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_final`
with tabla_inic as (
    SELECT 
        fecha_proceso AS processed_date,
        CASE WHEN red IS NULL OR red = 'AUNA' THEN 'RED AUNA' ELSE red END AS red,
        num_factura AS num_factura_documento_ocr,
        num_siniestro,
        proveedor,
        sede,
        MAX(CASE WHEN estado_alerta_IA = 'OBSERVADA' THEN num_factura END) AS factura_observado,
        MAX(CASE WHEN estado_alerta_IA = 'OBSERVADA' THEN monto_factura END) AS monto_observado,
        MAX(monto_factura) AS monto_total
    FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
    WHERE fecha_proceso = PERIODO_INI
    GROUP BY processed_date, red,   num_factura_documento_ocr, num_siniestro, proveedor, sede

),
tabla_potencial as 

(
SELECT 
        num_factura AS num_factura_documento_ocr,
        num_siniestro,
        monto_factura AS monto_factura_trama,
        flag_reglas_ahorro AS flag_pot_ahorro,
        estado_alerta_IA
--select *
from  `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final` 
),
tabla_ahorro_fin as (
  select num_factura_documento_ocr,
  num_siniestro,
  count(1) num_pot,
  max(monto_factura_trama) monto_potencial,
  --max(case when estado_alerta_IA='OBSERVADA' then monto_factura_trama end) monto_observado
  from tabla_potencial
  where flag_pot_ahorro = 1
  GROUP BY num_factura_documento_ocr, num_siniestro
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
    red,
    COUNT(num_factura_documento_ocr) AS ctdFacturas,
    SUM(monto_total) AS mtoFacturado,
    COUNT(factura_observado) AS ctdFactObservadas,
    SAFE_DIVIDE(COUNT(factura_observado), COUNT(num_factura_documento_ocr)) AS pctFactObs,
    SUM(monto_observado) AS mtoObservado,
    SUM(monto_potencial) AS mtoPotAhorro,
    SAFE_DIVIDE(SUM(monto_potencial), SUM(monto_observado)) AS pctMtoAhorrovsObservador 
 from tabla_final
 GROUP BY ALL
 ORDER BY processed_date DESC
 ;



#################################################################
############### 5. FORMATO DEL RESUMEN CORREO ENVIO #############
#################################################################
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.resumen_correo_diario_auditoria_final`

-- Esto es una temporal, normal que se chanque
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.resumen_correo_diario_auditoria_final`
AS
SELECT
  * EXCEPT(processed_date)
FROM `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_final`
WHERE processed_date = PERIODO_INI
AND red = 'RED AUNA'
; -- ACTUALIZAR FECHA O PONER EL MAX processed_date


#################################################################
############### 6. ACTUALIZAR TABLA DE RESUMEN CORREO ###########
#################################################################
-- Eliminamos los registros de esa fecha, antes de el insert, por si acaso
DELETE FROM `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_final_rechazo`
WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
;

--CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_final_rechazo` AS
INSERT INTO `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_final_rechazo`
WITH tabla_inic AS (
    SELECT 
        fecha_proceso AS processed_date,
        CASE WHEN red IS NULL THEN 'RED AUNA' ELSE red END AS red,
        num_factura AS num_factura_documento_ocr,
        num_siniestro,
        proveedor,
        sede,
        MAX(CASE WHEN estado_alerta_IA = 'OBSERVADA' THEN num_factura END) AS factura_observado,
        MAX(CASE WHEN estado_alerta_IA = 'OBSERVADA' THEN monto_factura END) AS monto_observado,
        MAX(monto_factura) AS monto_total
    FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
    WHERE fecha_proceso = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
    AND flag_reglas_ahorro = 1
    GROUP BY processed_date, red,   num_factura_documento_ocr, num_siniestro, proveedor, sede
),
tabla_potencial AS (
    SELECT 
        num_factura AS num_factura_documento_ocr,
        num_siniestro,
        monto_factura AS monto_factura_trama,
        flag_reglas_ahorro AS flag_pot_ahorro,
        estado_alerta_IA
    FROM `{{project_id}}.siniestro_salud_auna.reporte_auditoria_final`
),
tabla_ahorro_fin AS (
    SELECT 
        num_factura_documento_ocr,
        num_siniestro,
        COUNT(1) AS num_pot,
        MAX(monto_factura_trama) AS monto_potencial
    FROM tabla_potencial
    WHERE flag_pot_ahorro = 1
    GROUP BY num_factura_documento_ocr, num_siniestro
),
tabla_final AS (
    SELECT 
        a.*,
        b.num_factura_documento_ocr AS factura_potencial,
        b.num_pot,
        b.monto_potencial
    FROM tabla_inic a
    LEFT JOIN tabla_ahorro_fin b
    ON a.num_factura_documento_ocr = b.num_factura_documento_ocr 
    AND a.num_siniestro = b.num_siniestro
)
SELECT 
    processed_date,
    red,
    COUNT(num_factura_documento_ocr) AS ctdFacturas,
    SUM(monto_total) AS mtoFacturado,
    COUNT(factura_observado) AS ctdFactObservadas,
    SAFE_DIVIDE(COUNT(factura_observado), COUNT(num_factura_documento_ocr)) AS pctFactObs,
    SUM(monto_observado) AS mtoObservado,
    SUM(monto_potencial) AS mtoPotAhorro,
    SAFE_DIVIDE(SUM(monto_potencial), SUM(monto_observado)) AS pctMtoAhorrovsObservador
FROM tabla_final
GROUP BY ALL
ORDER BY processed_date DESC
;


##############################################################################
############### 7. FORMATO DEL RESUMEN CORREO ENVIO - 2DA PARTE ##############
##############################################################################
-- Esto es para los primeros bullets del correo (AUNA)
-- Esto es una temporal, normal que se chanque
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.resumen_correo_diario_auditoria_final_rechazo`
AS
SELECT
  processed_date,
  SUM(ctdFacturas) AS ctdFacturas,
  SUM(mtoFacturado) AS mtoFacturado,
  SUM(ctdFactObservadas) AS ctdFactObservadas,
  (SUM(ctdFactObservadas) / SUM(ctdFacturas)) AS pctFactObs,
  SUM(mtoObservado) AS mtoObservado,
  SUM(mtoPotAhorro) AS mtoPotAhorro,
  (SUM(mtoObservado) / SUM(mtoPotAhorro)) AS pctMtoAhorrovsObservador
FROM `{{project_id}}.siniestro_salud_auna.resumenes_correos_auditoria_final_rechazo`
WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
GROUP BY ALL
;