################## E2_01_FAB_SS_Consolidado_OCR ###########################

# Nombre Query: E2_01_FAB_SS_Consolidado_OCR
# Objetivo: Consolidar la información de las hojas del OCR (Factura, SITEDS)
# Objetivos: 
# - O1: Limpiar las fechas (calidad de datos)
# - O2: Delimitar los casos según fecha de emisión solicitada
# - O2: Obtener la factura única (fecha más reciente)

######################################################################

####################################
######### Fuentes Usadas ###########
####################################
-- 1. `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve` -- Esto fue creado en un query de limpieza
-- 2. `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico` -- Esto fue creado en un query de limpieza
-- 3. `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito` -- Esto fue creado en un query de limpieza
-- 4. `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion` -- Esto fue creado en un query de limpieza
-- 5. `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion` -- Esto fue creado en un query de limpieza
-- 6. `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia` -- Esto fue creado en un query de limpieza
-- 7. `{{project_id}}.siniestro_salud_auna.ocr_data_epicrisis` -- Esto fue creado en un query de limpieza
-- 8. `{{project_id}}.siniestro_salud_auna.ocr_data_receta` -- Esto fue creado en un query de limpieza
-- 9. `{{project_id}}.siniestro_salud_auna.trama_notacredito_previa` -- Creado en query de trama
-- 10. `{{project_id}}.siniestro_salud_auna.trama_cartagarantia_previa` -- Creado en query de trama
-- 11. `{{project_id}}.siniestro_salud_auna.trama_factura_rfs`  -- Creado en query de trama
-- 12. `{{project_id}}.siniestro_salud_auna.trama_sited_previa`  -- Creado en query de trama
######################################

DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

#################################################################################################################
#### PASO 1: Unificar las 8 tablas Factura, SITEDS, NC, Preliquidacion, CG, Liquidacion, Epicrisis y Receta #####
#################################################################################################################

-- select * from `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado` WHERE nro_clean is not null
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
AS 
SELECT 
  DISTINCT *,
  CASE
    WHEN B.num_factura_siteds_ocr IS NULL THEN 1
    ELSE 0
  END AS flag_sinSiteds -- select *
  FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_breve` AS A
LEFT JOIN `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico` AS B
ON A.num_factura_documento_ocr = B.num_factura_siteds_ocr AND A.ruc_proveedor_emisor_ocr = B.ruc_emisor_path
LEFT JOIN 
(
  SELECT * EXCEPT(page_path) FROM `{{project_id}}.siniestro_salud_auna.ocr_data_notaCredito`
) AS C
ON A.num_factura_documento_ocr = C.num_factura_notacredito_ocr --AND A.ruc_proveedor_emisor_ocr = B.RUC_TRAMAS
LEFT JOIN 
(
  SELECT * --EXCEPT(page_path, documento_id, created_at) 
  FROM `{{project_id}}.siniestro_salud_auna.ocr_data_preLiquidacion`
) AS D
ON A.num_factura_documento_ocr = D.num_factura_preliqui_ocr --AND A.ruc_proveedor_emisor_ocr = B.RUC_TRAMAS
LEFT JOIN 
(
  SELECT * FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion`
) AS E
ON A.num_factura_documento_ocr = E.num_factura_liqui_ocr AND A.ruc_proveedor_emisor_ocr = E.ruc_liqui_ocr
LEFT JOIN 
(
  SELECT * EXCEPT(page_path, documento_id, created_at) FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`
) AS F
ON A.num_factura_documento_ocr = F.num_factura_cartagarantia_ocr AND A.ruc_proveedor_emisor_ocr = F.ruc_cg_ocr
LEFT JOIN 
(
  SELECT * FROM `{{project_id}}.siniestro_salud_auna.ocr_data_epicrisis`
) AS G
ON A.num_factura_documento_ocr = G.num_factura_epicrisis_ocr AND A.ruc_proveedor_emisor_ocr = G.ruc_epi_ocr
LEFT JOIN 
(
  SELECT * FROM `{{project_id}}.siniestro_salud_auna.ocr_data_receta`
) AS H
ON A.num_factura_documento_ocr = H.num_factura_rc_ocr AND A.ruc_proveedor_emisor_ocr = H.ruc_rc_ocr
WHERE A.ruc_proveedor_emisor_ocr IS NOT NULL
;

-- SELECT COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
-- SELECT COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.ocr_data_cartaGarantia`


------------- CAMPOS DE NOTA DE CREDITO (USANDO TRAMA) -------------
---------- Le asignamos el número de factura, el monto final de la TRAMA Nota de Crédito y la fecha de emisión de trama
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
AS 
SELECT 
  A.*,
  -- Trama: Numero de factura
  B.numero_de_documento_de_pago as numero_de_documento_de_pago_trama,
  -- Trama: Monto final
  B.MONTONOTA as montoSub_nc_trama,
  -- Trama: Fecha de emision
  B.FECHANOTA as fecha_nc_trama,
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado` AS A
LEFT JOIN (
  SELECT numero_de_documento_de_pago, fechanota, montonota, ruc_tramaNC FROM `{{project_id}}.siniestro_salud_auna.trama_notacredito_previa`
  WHERE TIPONOTA = 'C'
) as B
ON (REPLACE(A.num_factura_documento_ocr, '-', '') = B.numero_de_documento_de_pago) AND A.ruc_proveedor_emisor_ocr = B.ruc_tramaNC
;


-- Añadir si cuenta con NC en la trama (Si tiene NC trama, deberia estar en el sustento)
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
AS 
SELECT 
  DISTINCT A.*,
  -- Acá poner los campos necesarios de la tabla nota de credito (usar * cuando se tenga mapeado)
  CASE
    WHEN A.numero_de_documento_de_pago_trama IS NOT NULL THEN 1 ELSE 0
  END AS flag_conNC_trama,
  -- Flag si el monto NC OCR coincide con monto de Trama
  CASE
    WHEN A.importe_total_ocr_nc = A.montoSub_nc_trama THEN 1 ELSE 0
  END AS flag_montoFinal_NC_coicinde,
  -- Monto factura OCR - Monto NC OCR
  (monto_sub_factura_ocr - montoSub_nc_trama) AS dif_montoSubFact_montoSubNC_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado` AS A
;



------------ VALIDACION FACTURAS DUPLICADAS (debería salir tabla vacía)
WITH facturas_duplicadas AS (
  SELECT 
    num_factura_documento_ocr,
    COUNT(*) as conteo
  FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
  GROUP BY num_factura_documento_ocr
  HAVING COUNT(*) > 1
  --ORDER BY 2 DESC
)
SELECT t.*
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado` t
JOIN facturas_duplicadas fd ON t.num_factura_documento_ocr = fd.num_factura_documento_ocr
ORDER BY t.num_factura_documento_ocr -- puedes ordenar por otros campos relevantes
;



------------- CAMPOS DE CARTA GARANTIA (USANDO TRAMA) -------------
---------- Le asignamos el número de factura de la TRAMA Carta de Garantia
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
AS 
SELECT 
  A.*,
  B.num_factura_trama_carta,
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado` AS A
LEFT JOIN (
  SELECT num_factura_trama_carta, ruc_tramaCG FROM `{{project_id}}.siniestro_salud_auna.trama_cartagarantia_previa`
) as B
ON (REPLACE(A.num_factura_documento_ocr, '-', '') = B.num_factura_trama_carta) AND A.ruc_proveedor_emisor_ocr = B.ruc_tramaCG
;


-- Añadir si cuenta con CG en la trama (Si tiene CG trama, deberia estar en el sustento)
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
AS 
SELECT 
  DISTINCT A.*,
  -- Acá poner los campos necesarios de la tabla carta garantia (usar * cuando se tenga mapeado)
  CASE
    WHEN A.num_factura_trama_carta IS NOT NULL THEN 1 ELSE 0
  END AS flag_conCG_trama
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado` AS A
;



------------- CAMPOS DE LIQUIDACION (USANDO TRAMA FACTURA) -------------
---------- Le asignamos el subtotal y el mecanismo de pago de la trama factura
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
AS 
SELECT 
  A.*,
  B.subtotal_tramas as subtotal_fact_trama,
  B.descripcion_tramas as mecanismo_pago_fact_trama,
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado` AS A
LEFT JOIN (
  SELECT numero_de_documento_de_pago, ruc_tramas, subtotal_tramas, descripcion_tramas FROM `{{project_id}}.siniestro_salud_auna.trama_factura_rfs`
) as B
ON (REPLACE(A.num_factura_documento_ocr, '-', '') = B.numero_de_documento_de_pago) AND A.ruc_proveedor_emisor_ocr = B.ruc_tramas
;



------------- CAMPOS DE SITEDS (USANDO TRAMA SITEDS) -------------
---------- Le asignamos los 3 cie10 de la trama siteds
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
AS 
SELECT 
  A.*,
  REPLACE(B.CODIGOCIE101, '.', '') as cod_cie101_trama_sited,
  REPLACE(B.CODIGOCIE102, '.', '') as cod_cie102_trama_sited,
  REPLACE(B.CODIGOCIE103, '.', '') as cod_cie103_trama_sited,
  -- Nos traemos el campo TIPOCOBERTURA de la trama 2
  CASE 
    CAST(TIPOCOBERTURA AS INT64)
    WHEN 4 THEN 'AMBULATORIO'
    WHEN 0  THEN 'OTROS'
    WHEN 1  THEN 'HOSPITALARIO'
    WHEN 2  THEN 'OTROS'
    WHEN 3  THEN 'OTROS'
    WHEN 5  THEN 'HOSPITALARIO'
    WHEN 6  THEN 'EMERGENCIA'
    WHEN 9  THEN 'OTROS'
    ELSE 'OTROS'
  END AS cobertura_siteds_trama
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado` AS A
LEFT JOIN (
  SELECT num_factura_trama_sited, RUCIPRESS, CODIGOCIE101, CODIGOCIE102, CODIGOCIE103, TIPOCOBERTURA FROM `{{project_id}}.siniestro_salud_auna.trama_sited_previa`
) as B
ON (REPLACE(A.num_factura_documento_ocr, '-', '') = B.num_factura_trama_sited) AND A.ruc_proveedor_emisor_ocr = B.RUCIPRESS
;



###################################################################################################
###################### PASO 2: Limpiar el consolidado para K  #####################################
###################################################################################################
--CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of`
--AS 
CREATE OR REPLACE TABLE  `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of` AS
SELECT 
  REPLACE(num_factura_documento_ocr, '-', '') AS num_factura_documento_ocr,
  REPLACE(num_factura_siteds_ocr, '-', '') AS num_factura_siteds_ocr,
  fecha_emision_fact_limpio_val,
  DATE(TIMESTAMP(fecha_emision_siteds_ocr)) AS fecha_emision_siteds_ocr,
  --cobertura_siteds_ocr_limpia as cobertura_siteds_ocr,
  * 
  EXCEPT(num_factura_documento_ocr, num_factura_siteds_ocr, fecha_emision_siteds_ocr, fecha_emision_fact_limpio_val, ruc_emisor_path
  --cobertura_siteds_ocr_limpia
  )
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado`
;

-- SELECT COUNT(DISTINCT num_factura_documento_ocr) FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of`



###################################################################################################
################### PASO 3: Tabla completa Consolidado para comparativo  ##########################
###################################################################################################

-- Eliminamos los registros de esa fecha, antes de el insert, por si acaso
DELETE FROM {{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial
WHERE processed_date = PERIODO_INI-- ACTUALIZAR FECHA O PONER EL MAX processed_date
;

INSERT INTO `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial`
SELECT DISTINCT 
  * EXCEPT (flag_ruc_invalido, --flag_codigo_autorizacion_invalido, 
  flag_poliza_ocr_invalida, flag_poliza_invalida, 
  flag_tipo_doc_valido,	flag_codigo_cmp_valido,	flag_tipo_afiliacion_valido, 
  flag_doc_paciente_siteds_invalido,
  flag_consultaAmboMed_liqui_ocr
  ),
  flag_consultaAmboMed_liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of`
WHERE TRUE
;



################################################################################################
################# PASO 4: Unificar las 2 tablas Liquidacion y SITEDS netamente CPM #############
################################################################################################
-- select * from `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_unico` WHERE num_factura_siteds_ocr = 'F775-00084983'
-- select * from `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado` WHERE num_factura_documento_ocr = 'F775-00084983'
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_liquidacion_siteds_consolidado_cpm`
AS 
SELECT 
  DISTINCT 
  C.processed_date,
  A.*,
  B.*,
  CASE
    WHEN B.num_factura_siteds_ocr IS NULL THEN 1
    ELSE 0
  END AS flag_sinSiteds -- select *
  FROM `{{project_id}}.siniestro_salud_auna.ocr_data_liquidacion_cpm` AS A
LEFT JOIN `{{project_id}}.siniestro_salud_auna.ocr_data_siteds_cpm` AS B
ON A.num_factura_liqui_ocr = B.num_factura_siteds_ocr AND A.ruc_liqui_ocr = B.ruc_emisor_path AND A.cod_autorizacion_liqui_ocr = B.num_siteds_ocr_val
LEFT JOIN ( -- Esto es para traernos netamente processed_date
  SELECT 
    num_factura_liqui_ocr, ruc_liqui_ocr, MAX(processed_date) AS processed_date 
  FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial`
  GROUP BY ALL
) AS C
ON A.num_factura_liqui_ocr = C.num_factura_liqui_ocr AND A.ruc_liqui_ocr = C.ruc_liqui_ocr
WHERE A.ruc_liqui_ocr IS NOT NULL
;


###################################################################################################
################# PASO 5: Tabla completa Consolidado para comparativo CPM #########################
###################################################################################################
-- Eliminamos los registros de esa fecha, antes de el insert, por si acaso
DELETE FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_liquidacion_siteds_consolidado_of_oficial_cpm`
WHERE processed_date BETWEEN '2025-10-21' AND '2025-10-21' -- ACTUALIZAR FECHA O PONER EL MAX processed_date
;

-- SELECT processed_date, count(*) FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial` group by all order by 1 desc

INSERT INTO `{{project_id}}.siniestro_salud_auna.auna_ocr_liquidacion_siteds_consolidado_of_oficial_cpm`
-- CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_liquidacion_siteds_consolidado_of_oficial_cpm` AS
SELECT DISTINCT 
  * EXCEPT (--flag_codigo_autorizacion_invalido, 
  flag_poliza_invalida, 
  flag_tipo_doc_valido,	flag_codigo_cmp_valido,	flag_tipo_afiliacion_valido, 
  flag_doc_paciente_siteds_invalido
  ),-- SELECT *
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_liquidacion_siteds_consolidado_cpm`
WHERE TRUE
;



------------------------ BACKUP SIN HABER PROCESADO LOS LOTES DE TODAS LAS FECHAS (SOLO 07)
-- CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial_backup`
-- AS
-- SELECT DISTINCT 
--   *
-- FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial`
-- ;

