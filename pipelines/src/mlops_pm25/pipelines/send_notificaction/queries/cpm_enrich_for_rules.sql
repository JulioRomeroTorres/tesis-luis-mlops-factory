####################################
######### Fuentes Usadas ###########
####################################
-- 1. `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial` -- Query de consolidado
-- 2. `rs-prd-dlk-dd-rawzone-a40f.bdrsa__app_eps.DOCUMENTO` 
-- 3. `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud`
-- 4. `{{anl_project_id}}.anl_persona.cliente_persona_detalle`
-- 5. `{{anl_project_id}}.anl_siniestro.carta_garantia_solicitud`
-- 6. `{{anl_project_id}}.anl_siniestro.siniestro_detalle_generales`
-- 7. `{{project_id}}.tmp.stg_modelo_finanzas_cuenta_por_pagar`
-- 8. `{{project_id}}.siniestro_salud_auna.pdfs_auna_reglas` -- Esto viene del query de PaymentDuplicity
-- 9. `{{project_id}}.siniestro_salud_auna.auna_listado_empresas_autoseguros` -- Esto viene del query de enrich for autoseguros
######################################

DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);


-- select * from `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas` limit 1
# Antes de hacer el insert, eliminamos los registros de la fecha actual por si acaso
DELETE FROM `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas_cpm`
WHERE processed_date = PERIODO_INI
;


-- SELECT processed_date, COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas` GROUP BY ALL ORDER BY 1 DESC

INSERT INTO `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas_cpm`
--CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas_cpm` AS
 
with
tabla_ini as (
select 
  processed_date,
  num_factura_liqui_ocr,
  ruc_liqui_ocr,
  cod_autorizacion_liqui_ocr,
  num_documento_liqui_ocr,
  monto_total_liqui_ocr,
  fec_ingreso_liqui_ocr,
  fec_alta_liqui_ocr,
  gastos_afectos_subtotal2_liqui_ocr,
  gastos_afectos_subtotal3_liqui_ocr,
  gastos_afectos_coaseguroPaciente_liqui_ocr,
  pct_coaseguro_liqui_ocr,
  deducible_liqui_ocr,
  mecanismo_liqui_ocr,
  flag_consultaAmboMed_liqui_ocr,
  page_path,
  num_factura_siteds_ocr,
  ruc_emisor_path,
  cobertura_siteds_ocr_limpia,
  codigoproducto_trama,
  producto_siteds_mod,
  producto_siteds_ocr,
  -- Estos son los productos de Rimac EPS: EPS (Planes Médicos), SCTR y ASEGURAMIENTO UNIVERSAL 
  CASE 
    WHEN UPPER(producto_siteds_ocr) LIKE '%EPS%' OR UPPER(producto_siteds_ocr) LIKE '%PLANES%' OR UPPER(producto_siteds_ocr) LIKE '%ASEGURAMIENTO%' OR UPPER(producto_siteds_ocr) LIKE '%SCTR%' THEN 1 ELSE 0 
  END AS flag_producto_eps_siteds_ocr,
  cobertura_copago_variable_siteds_ocr,
  copago_fijo_siteds_ocr,
  fecha_emision_siteds_ocr,
  razon_social_contratante_siteds_ocr,
  cie10_siteds_ocr,
  num_poliza_siteds_ocr,
  tipo_documento_limpio,
  codigo_cmp_limpio,
  tipo_afiliacion_ocr,
  num_siteds_ocr_val,
  num_documento_paciente_siteds_ocr_val,
  fecha_de_prestacion,
  flag_sinSiteds,

-- select max(processed_date) -- select * 
FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_liquidacion_siteds_consolidado_of_oficial_cpm` 
WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date

),

  ClientRSSystem AS(
  select 
    distinct
    concat(cod_serie_comprobante_siniestro,lpad(num_comprobante_siniestro,8,'0')) factura,
    num_documento_proveedor_siniestro,
    nom_comercial_proveedor_siniestro,
    nom_sede_proveedor_siniestro,
    num_carta_garantia,
    --siniestro.id_autorizacion,
    num_documento_afiliado,
    id_persona_afiliado,
    cod_mecanismo_pago,
    des_mecanismo_pago,
    id_cobertura_origen,
    CASE
      WHEN ats.id_cobertura_origen IN ("P09","A70","A71","R29","Q05","L12") THEN "AMBULATORIO"
      WHEN ats.id_cobertura_origen IN ("R24","R25","R26","R27","R28","A74","V21") THEN "PREVENCION"
      WHEN ats.id_cobertura_origen IN ("H05","M20") THEN "HOSPITALARIO"
      WHEN ats.id_cobertura_origen IN ("E18") THEN "EMERGENCIA"
      WHEN ats.id_cobertura_origen IN ("O11") THEN "ONCOLOGIA"
      ELSE TRIM(ats.agrupacion_cobertura_negocio)
    END AS agrupacion_cobertura_negocio,
    des_producto_agrupado,
    mnt_nota_credito,
    -- CASE 
    --   WHEN REGEXP_CONTAINS(num_documento_afiliado, r'^[0-9]+$') THEN LPAD(CAST(CAST(num_documento_afiliado AS INT64) AS STRING), 8,'0') ELSE num_documento_afiliado END 
    -- AS num_documento_afiliado
  FROM `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud` siniestro
  LEFT join unnest(atencion_salud) ats
  WHERE siniestro.periodo = DATE_TRUNC(current_date(),month) and  substr(id_siniestro,4,1)<>'9' 
  ),
  
  -- #############################################################################
  -- #                        CTEs de Procesamiento y Uniones
  -- #############################################################################

  -- Reglas de Liquidacion
  reglas_liquidacion AS
  (
    SELECT 
      *,
      CASE -- Fecha de Ingreso > Fecha alta (no debería pasar)
        WHEN ( DATE(fec_ingreso_liqui_ocr) > DATE(fec_alta_liqui_ocr) ) THEN 1 ELSE 0
      END AS flag_fecIngreso_mayor_fecAlta_liqui_ocr,
    FROM tabla_ini
  ),

  reglas_consolidado AS
  (
    SELECT 
      A.*,
      B.monto_factura_ocr,
      CASE -- El Monto Fact OCR coincide con Monto Liquidacion OCR (margen de 0.1 favoreciendo Liquidacion) -- 
        WHEN ( (A.monto_total_liqui_ocr - B.monto_factura_ocr) > 0.1 AND A.num_factura_liqui_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_montoTotal_NoCoincide_Fact_Liqui_ocr,
    FROM tabla_ini AS A
    LEFT JOIN `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial` AS B
    ON A.num_factura_liqui_ocr = B.num_factura_liqui_ocr AND A.ruc_liqui_ocr = B.ruc_proveedor_emisor_ocr
  ),

  reglas_cross AS
  (
    SELECT 
      *, 
      
      -- Alertar en caso Siteds sea posterior a Liquidacion en más de 7 días
      CASE -- 
        WHEN (DATE_DIFF(fecha_emision_siteds_ocr, fec_ingreso_liqui_ocr, DAY) > 7 AND num_factura_liqui_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_fecIngreso_liqui_ocr_menor_fecAutoSiteds_ocr,
      
      -- Alertar en caso Liquidacion sea posterior a Siteds desde 15 días
      CASE --  
        WHEN DATE_DIFF(DATE(fec_ingreso_liqui_ocr), DATE(fecha_emision_siteds_ocr), DAY) > 14 AND num_factura_liqui_ocr IS NOT NULL
 THEN 1 ELSE 0
      END AS flag_fecIngreso_liqui_ocr_mayor15d_fecAutoSiteds_ocr,
      
      CASE -- DNI de la hoja SITEDS coincide con DNI de la hoja LIQUIDACION
        WHEN (num_documento_paciente_siteds_ocr_val <> num_documento_liqui_ocr AND num_factura_liqui_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_dniPaciente_Siteds_ocr_NoCoincide_Liqui_ocr,
      
      CASE -- Cod Autorizacion de la hoja SITEDS coincide con cod autorizacion de la hoja LIQUIDACION (Cuando es CPM que no se alerte)
        WHEN (num_siteds_ocr_val <> cod_autorizacion_liqui_ocr AND num_factura_liqui_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_codAutorizacion_Siteds_ocr_NoCoincide_Liqui_ocr,

      CASE -- El copago de SITEDS coincide con el deducible de LIQUIDAION
        WHEN (copago_fijo_siteds_ocr <> deducible_liqui_ocr AND num_factura_liqui_ocr IS NOT NULL 
        AND flag_consultaAmboMed_liqui_ocr = 1) THEN 1 ELSE 0
      END AS flag_deduciblePaciente_Siteds_ocr_NoCoincide_Liqui_ocr,


    FROM reglas_consolidado
  )

 select *
 from  reglas_cross
;



###################################################################################################
################### PASO 1: Tabla completa Consolidado para comparativo  ##########################
###################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas_cpm_grouped` 
AS
SELECT 
  processed_date,
  num_factura_liqui_ocr,
  ruc_liqui_ocr,
  MAX(flag_consultaAmboMed_liqui_ocr) AS flag_consultaAmboMed_liqui_ocr,
  MIN(flag_montoTotal_NoCoincide_Fact_Liqui_ocr) AS flag_montoTotal_NoCoincide_Fact_Liqui_ocr,
  MAX(flag_fecIngreso_liqui_ocr_menor_fecAutoSiteds_ocr) AS flag_fecIngreso_liqui_ocr_menor_fecAutoSiteds_ocr,
  MAX(flag_fecIngreso_liqui_ocr_mayor15d_fecAutoSiteds_ocr) AS flag_fecIngreso_liqui_ocr_mayor15d_fecAutoSiteds_ocr,
  MAX(flag_dniPaciente_Siteds_ocr_NoCoincide_Liqui_ocr) AS flag_dniPaciente_Siteds_ocr_NoCoincide_Liqui_ocr,
  MAX(flag_codAutorizacion_Siteds_ocr_NoCoincide_Liqui_ocr) AS flag_codAutorizacion_Siteds_ocr_NoCoincide_Liqui_ocr,
  MAX(flag_deduciblePaciente_Siteds_ocr_NoCoincide_Liqui_ocr) AS flag_deduciblePaciente_Siteds_ocr_NoCoincide_Liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas_cpm`
GROUP BY ALL
;

------------- BACKUP RULES
-- CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas_backup_20250915`
-- AS
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas`
-- ;