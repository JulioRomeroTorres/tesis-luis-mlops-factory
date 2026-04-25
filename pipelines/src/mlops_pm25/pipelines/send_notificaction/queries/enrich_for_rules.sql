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

DELETE FROM `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas`
WHERE processed_date = PERIODO_INI;

INSERT INTO
  `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas`
  
with
tabla_ini as (
select 
processed_date,
num_factura_documento_ocr,
dni_factura_ocr,
flag_compania_factura_ocr,
ruc_proveedor_emisor_ocr,
poliza_factura_ocr,
fecha_emision_fact_limpio_val,
mecanismo_pago_fact_trama,
subtotal_fact_trama,
num_siteds_ocr_val,
--cobertura_siteds_ocr,	
cobertura_siteds_ocr_limpia,	
producto_siteds_ocr,
-- Estos son los productos de Rimac EPS: EPS (Planes Médicos), SCTR y ASEGURAMIENTO UNIVERSAL 
CASE 
  WHEN UPPER(producto_siteds_ocr) LIKE '%EPS%' OR UPPER(producto_siteds_ocr) LIKE '%PLANES%' OR UPPER(producto_siteds_ocr) LIKE '%ASEGURAMIENTO%' OR UPPER(producto_siteds_ocr) LIKE '%SCTR%' THEN 1 ELSE 0 
END AS flag_producto_eps_siteds_ocr,
num_documento_paciente_siteds_ocr_val,
fecha_emision_siteds_ocr,
num_poliza_siteds_ocr,
codigoproducto_trama,
producto_siteds_mod,
copago_fijo_siteds_ocr,
cobertura_copago_variable_siteds_ocr,
razon_social_contratante_siteds_ocr,
ruc_compania_factura_ocr,
monto_sub_factura_ocr,
monto_factura_ocr,
monto_factura_trama,
razon_social_fact_limpia_ocr,
importe_total_ocr_nc,
montoSub_nc_trama,
monto_total_preliqui_ocr,
monto_total_liqui_ocr,
flag_sinSiteds,
flag_conNC_trama,
fecha_nc_limpia,
fecha_nc_trama,
num_factura_notaCredito_ocr,
subtotal_ocr_nc,
num_factura_preliqui_ocr,
fec_ingreso_preliqui_ocr,
fec_alta_preliqui_ocr,
num_factura_liqui_ocr,
fec_ingreso_liqui_ocr,
fec_alta_liqui_ocr,
mecanismo_liqui_ocr,
gastos_afectos_subtotal2_liqui_ocr,
gastos_afectos_subtotal3_liqui_ocr,
gastos_afectos_coaseguroPaciente_liqui_ocr,
deducible_liqui_ocr,
num_documento_liqui_ocr,
cod_autorizacion_liqui_ocr,
num_factura_cartagarantia_ocr,
nro_carta,
flag_conCG_trama,
monto_final_cg,
razon_social_proveedor_cg_limpia_ocr,
ruc_proveedor_cg_ocr,
num_factura_epicrisis_ocr,
fec_ingreso_epi_ocr,
fec_egreso_epi_ocr,
dias_hosp_epi_ocr,
dx_ingreso_epi_ocr, --
cobertura_siteds_trama,
cie10_siteds_ocr,
cod_cie101_trama_sited,
cod_cie102_trama_sited,
cod_cie103_trama_sited,
num_factura_rc_ocr,
flag_medicamento_factura_ocr,
dx_entrada_transformada_cg_ocr,
pct_coaseguro_liqui_ocr,
flag_consultaAmboMed_liqui_ocr,
-- Fechas de CG
fec_emision as fec_emision_cg_ocr,
fec_val_sol as fec_validez_cg_ocr,
-- SELECT distinct num_factura_documento_ocr FROM `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas` 
-- select distinct processed_date from `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial`

FROM `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial` 
WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY num_factura_documento_ocr ORDER BY 
      fecha_emision_siteds_ocr IS NULL,fecha_emision_siteds_ocr asc) = 1
),
  -- KnownDocuments_RS AS (-- de acá solo saco flag_factura_conocida
  --   SELECT
  --     NRO_DOCUMENTO_PRD
  --   FROM
  --     `rs-prd-dlk-dd-rawzone-a40f.bdrsa__app_eps.DOCUMENTO`
  --   WHERE
  --     FORMA_RECLAMO = 'C'
  --     AND COD_PROVEEDOR IN (8227559, 276471, 116520, 8920981, 8637298, 422739, 629, 1237)
  -- ),

  -- KnownDocuments_RS AS (
  -- SELECT
  --   CONCAT(cod_serie_comprobante_siniestro, LPAD(num_comprobante_siniestro, 8, '0')) AS numero_factura
  -- FROM `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud`
  -- WHERE TRUE
  -- AND des_tipo_reclamo = 'Credito'
  -- AND id_persona_proveedor_siniestro IN ('AX-116520', 'AX-276471', 'AX-8227559', 'AX-629', 'AX-1237', 'AX-8920981', 'AX-8637298', 'AX-422739'
  --   )
  -- ),

  -- siniestros_documento AS ( -- de acá saco cantidad de siniestros x CG lo cual sirve para flag_siniestroDoble mas adelante
  --   SELECT 
  --     NRO_CARTA,
  --     COUNT(DISTINCT(CONCAT(TIPO_ENTIDAD_SALUD, '-', COD_ENTIDAD_SALUD, '-', ANO_DOCUMENTO, '-', NRO_DOCUMENTO))) as cantidad
  --     --CONCAT(TIPO_ENTIDAD_SALUD, '-', COD_ENTIDAD_SALUD, '-', ANO_DOCUMENTO, '-', NRO_DOCUMENTO) AS id_siniestro_origen,
  --   FROM `rs-prd-dlk-dd-rawzone-a40f.bdrsa__app_eps.DOCUMENTO`
  --   WHERE NRO_GARANTIA IS NOT NULL
  --   GROUP BY ALL
  -- ),

  HistoricalClaimData AS ( -- de acá creo flag_siniestroDoble con raw y siniestros_documento
    SELECT DISTINCT
      id_siniestro,
      num_siniestro,
      id_autorizacion,
      REPLACE(id_autorizacion, 'RS-', '') AS cod_autorizacion,
      num_carta_garantia,
      a.num_obligacion,
      fec_hora_ocurrencia,
      num_documento_proveedor_siniestro,
      tip_documento_afiliado,
      num_documento_afiliado,
      tip_documento_titular,
      num_documento_titular,
      cod_serie_comprobante_siniestro,
      id_poliza,
      num_comprobante_siniestro,
      mto_auditado_sol,
      CONCAT(cod_serie_comprobante_siniestro, LPAD(num_comprobante_siniestro, 8, '0')) AS numero_factura,
      --COALESCE(CONCAT(cod_serie_comprobante_siniestro, LPAD(num_comprobante_siniestro, 8, '0')), doc.NRO_DOCUMENTO_PRD) AS numero_factura,
      --COUNT(COALESCE(CONCAT(cod_serie_comprobante_siniestro, LPAD(num_comprobante_siniestro, 8, '0')), doc.NRO_DOCUMENTO_PRD)),
      -- Flag siniestro doble
      -- CASE 
      --   WHEN sd.cantidad > 1 THEN 1 
      --   ELSE 0 
      -- END AS flag_siniestroDoble

    FROM `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud` AS a
    LEFT JOIN UNNEST(a.atencion_salud) AS atencion_salud
    LEFT JOIN UNNEST(atencion_salud.procedimiento_salud) AS procedimiento_salud
    LEFT JOIN UNNEST(procedimiento_salud.detalle_farmacia_insumo) AS detalle_farmacia_insumo
    -- LEFT JOIN `rs-prd-dlk-dd-rawzone-a40f.bdrsa__app_eps.DOCUMENTO` doc 
    --   ON a.id_siniestro_origen = CONCAT(doc.TIPO_ENTIDAD_SALUD, '-', doc.COD_ENTIDAD_SALUD, '-', doc.ANO_DOCUMENTO, '-', doc.NRO_DOCUMENTO)
    -- LEFT JOIN siniestros_documento sd
    --   ON a.num_carta_garantia = sd.NRO_CARTA
    WHERE
      TRUE
      AND periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
      AND tip_reclamo = 'C'
      AND SUBSTR(id_siniestro, 4, 1) <> '9'
  ),
  -- ClientPolicyData: Recupera y filtra detalles de clientes y sus pólizas de salud.
  -- Se utiliza para asociar las facturas a los planes de salud de los pacientes.
  ClientPolicyData AS (
    SELECT
      *,
      SUBSTR(cuc, 3) AS nro_documento_paciente -- Extrae el DNI del CUC
    FROM -- select distinct des_agrupacion_n2 from
      `{{anl_project_id}}.anl_persona.cliente_persona_detalle`
    WHERE
      des_riesgo = 'SALUD'
      AND id_estado_poliza IN ('ACT', 'NO DETERMINADO', 'NDE')
      AND id_estado_certificado IN ('ACT', 'NO DETERMINADO', 'NDE')
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
  HistoricalCG AS (
    SELECT 
      periodo,
      num_carta_garantia,
      num_carta_garantia_origen,
      num_documento_proveedor,
      id_siniestro,
      CASE WHEN des_est_carta_garantia_origen = 'ANULADA' THEN 1 ELSE 0 END AS flag_cg_anulada
    FROM `{{project_id}}.tmp.carta_garantia_solicitud`
    WHERE periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
  ),

  MaxValidDate_RS AS (
    SELECT
      DISTINCT
      id_persona, 
      REGEXP_EXTRACT(id_unidad_asegurable, r'^([^\\-]+-[^\\-]+)') AS id_poliza, 
      fec_inicio_vigencia, 
      fec_fin_vigencia,
      des_estado_origen_unidad_asegurable
    FROM (
      SELECT 
        id_persona, 
        id_unidad_asegurable,
        fec_inicio_vigencia, 
        fec_fin_vigencia,
        des_estado_origen_unidad_asegurable,
        REGEXP_EXTRACT(id_unidad_asegurable, r'^([^\\-]+-[^\\-]+)') AS id_poliza,
        ROW_NUMBER() OVER (
          PARTITION BY id_persona, REGEXP_EXTRACT(id_unidad_asegurable, r'^([^\\-]+-[^\\-]+)')
          ORDER BY 
            CASE WHEN des_estado_origen_unidad_asegurable = 'VIGENTE' THEN 1 ELSE 2 END,
            fec_inicio_vigencia DESC
        ) AS rn
      FROM `{{project_id}}.tmp.unidad_asegurable`
      WHERE id_origen = 'RS'
  )
  WHERE rn = 1
  ),

  -- PaidInvoiceStatus: Identifica si un siniestro ha sido pagado, obteniendo su estado de cuenta por pagar.
  -- PaidInvoiceStatus AS (
  --   SELECT DISTINCT
  --     a.id_siniestro,
  --     b.est_cuenta_por_pagar,
  --     b.des_est_cuenta_por_pagar,
  --     b.fec_est_cuenta_por_pagar,
  --     b.id_moneda,
  --     b.mnt_bruto_pago_moneda,
  --     b.mnt_neto_pago_moneda,
  --     b.num_cuenta_por_pagar,
  --     -- Extrae una parte del id_siniestro que parece ser el documento_factura
  --     SUBSTR(a.id_siniestro, LENGTH(RTRIM(a.id_siniestro, '-')) - STRPOS(REVERSE(RTRIM(a.id_siniestro, '-')), '-') + 2) AS documento_factura
  --   FROM (SELECT *
  --   FROM 
  --     `{{anl_project_id}}.anl_siniestro.siniestro_detalle_generales`  a
  --   INNER JOIN
  --     UNNEST(a.pre_liquidacion) p
  --   WHERE
  --     periodo = DATE_TRUNC(CURRENT_DATE(), MONTH) -- Periodo del mes actual
  --     AND id_origen = 'RS' ) A
  --   LEFT JOIN
  --     `{{project_id}}.tmp.stg_modelo_finanzas_cuenta_por_pagar` b
  --     ON a.id_cuenta_por_pagar = b.id_cuenta_por_pagar
  -- ),

  
  -- #############################################################################
  -- #                        CTEs de Procesamiento y Uniones
  -- #############################################################################

  -- ProcessedInvoiceData_Step1: Primera fase de procesamiento de los datos de factura extraídos.
  -- Realiza uniones iniciales con KnownDocuments_RS y HistoricalClaimData.
  -- También convierte la cadena de fecha de emisión (en formato libre con meses en español)
  -- a un formato de fecha estándar de BigQuery.
  ProcessedInvoiceData_Step1 AS (
    SELECT DISTINCT
      a.*,
      NULL AS flag_factura_conocida,
      -- CASE WHEN b.NRO_DOCUMENTO_PRD IS NOT NULL THEN 1 ELSE 0 END AS flag_factura_conocida,
      c.id_siniestro,
      c.id_poliza,
      c.num_siniestro,
      c.mto_auditado_sol,
      null as flag_siniestroDoble
      -- c.flag_siniestroDoble
    FROM
      tabla_ini AS a
    -- LEFT JOIN
    --   KnownDocuments_RS AS b
    --   ON (a.num_factura_documento_ocr = b.NRO_DOCUMENTO_PRD) -- Proveedor, afiliado, ocurrencia y la factura.
    LEFT JOIN
      HistoricalClaimData AS c
      ON (
        a.num_factura_documento_ocr = c.numero_factura
        AND a.ruc_proveedor_emisor_ocr = c.num_documento_proveedor_siniestro
        --a.num_siteds_ocr_val = c.cod_autorizacion
        )
  )  ,

  -- ProcessedInvoiceData_Step2_WithPaymentStatus: Añade el estado de pago del siniestro.
  ProcessedInvoiceData_Step2_WithPaymentStatus AS (
    SELECT
      a.*,
      DATE_TRUNC(DATE(a.fecha_emision_fact_limpio_val), MONTH) AS periodo_emision,
      DATE_TRUNC(DATE(a.fecha_emision_siteds_ocr), MONTH) AS periodo_emision_sited,
      'NULL' AS des_est_cuenta_por_pagar,
      -- b.des_est_cuenta_por_pagar
    FROM
      ProcessedInvoiceData_Step1 AS a
    -- LEFT JOIN
    --   PaidInvoiceStatus AS b
    --   ON (a.id_siniestro = b.id_siniestro)
  ),

  -- ProcessedInvoiceData_Step3_WithClientPlan: Incorpora la agrupación de plan de salud del cliente.
  ProcessedInvoiceData_Step3_WithClientPlan AS (
    SELECT DISTINCT
      a.*,
      DATE_DIFF(
        fecha_emision_fact_limpio_val,
        a.fecha_emision_siteds_ocr,
        DAY
      ) AS diferencia_dias_fecha_factura_prestacion,
      b.des_agrupacion_n1,
      CASE
        WHEN b.nro_documento_paciente IS NULL THEN 0 ELSE 1 END
      AS flag_dni_perdetalle_coincide,
      CASE
        WHEN c.num_documento_afiliado IS NULL THEN 0 ELSE 1 END
      AS flag_dni_rs_coincide,
      CASE
        WHEN a.producto_siteds_ocr = c.des_producto_agrupado THEN 1 ELSE 0 END
      AS flag_producto_rs_coincide,
      c.cod_mecanismo_pago,
      c.nom_comercial_proveedor_siniestro,
      c.nom_sede_proveedor_siniestro,
      c.des_producto_agrupado,
      c.id_cobertura_origen,
      c.agrupacion_cobertura_negocio,
      c.num_documento_afiliado,
      c.id_persona_afiliado,
      c.mnt_nota_credito,
      d.flag_cg_anulada
    FROM
      ProcessedInvoiceData_Step2_WithPaymentStatus AS a
    LEFT JOIN ClientPolicyData AS b
    ON (a.num_documento_paciente_siteds_ocr_val = b.nro_documento_paciente AND a.periodo_emision_sited = b.periodo)
    LEFT JOIN ClientRSSystem AS c
    ON (a.num_factura_documento_ocr = c.factura AND a.ruc_proveedor_emisor_ocr = c.num_documento_proveedor_siniestro)
    LEFT JOIN HistoricalCG AS D
    ON SAFE_CAST(SPLIT(a.nro_carta, '-')[OFFSET(0)] AS INT64) = d.num_carta_garantia AND a.ruc_proveedor_emisor_ocr = d.num_documento_proveedor
  ),

  ProcessedInvoiceData_Step4_WithMaxValidDate AS
  (
    SELECT 
      A.*,
      B.fec_inicio_vigencia,
      B.fec_fin_vigencia,
    FROM ProcessedInvoiceData_Step3_WithClientPlan AS A
    LEFT JOIN MaxValidDate_RS AS B
    ON A.id_persona_afiliado = B.id_persona AND A.id_poliza = B.id_poliza
    --DATE_TRUNC(A.fecha_emision_siteds_ocr, MONTH) = B.periodo_bitacora
  ),

  -- InvoiceData_WithPlanCoincidenceFlag: Marca si hay una coincidencia de plan según el RUC y el tipo de plan.
  InvoiceData_WithPlanCoincidenceFlag AS (
    SELECT
      a.*,
      CASE
        WHEN ruc_compania_factura_ocr = '20414955020' AND des_agrupacion_n1 = 'PLANES MEDICOS' THEN 1
        WHEN ruc_compania_factura_ocr = '20100041953' AND des_agrupacion_n1 = 'ASISTENCIA MEDICA' THEN 1
        ELSE 0
      END AS coincide_plan
    FROM
      ProcessedInvoiceData_Step4_WithMaxValidDate AS a
  ),

  InvoiceData_WithPlanCoincidenceFlag_Anomalies AS (
    SELECT
      a.*,
      --CONCAT(a.ruc_proveedor_emisor_ocr, '-', a.cobertura_siteds_ocr_limpia) as concat_modelo,
      CONCAT(a.ruc_proveedor_emisor_ocr, '-', a.agrupacion_cobertura_negocio) as concat_modelo,
      COALESCE(m.modelo, 'M39') AS modelo,
      COALESCE(m.umbral,40000) umbral,
      #CASE WHEN a.monto_sub_factura_ocr > COALESCE(m.umbral, 6623.462500000001) THEN 1 ELSE 0 END
      CASE WHEN a.monto_sub_factura_ocr > COALESCE(m.umbral, 40000) THEN 1 ELSE 0 END AS flag_anomaly
    FROM
      InvoiceData_WithPlanCoincidenceFlag AS a
    LEFT JOIN (
      -- M39 is handled by the COALESCE default
      SELECT '20101039910-ONCOLOGIA' AS key, 'M1' AS modelo, 152.16 AS umbral UNION ALL
      SELECT '20501781291-AMBULATORIO', 'M2', 4175.59 UNION ALL
      SELECT '20454135432-AMBULATORIO', 'M3', 2685.45 UNION ALL
      SELECT '20100251176-AMBULATORIO', 'M4', 1643.06 UNION ALL
      SELECT '20546292658-AMBULATORIO', 'M5', 1533.48 UNION ALL
      SELECT '20501781291-EMERGENCIA', 'M6', 2982.07 UNION ALL
      SELECT '20102756364-AMBULATORIO', 'M7', 1843.25 UNION ALL
      SELECT '20394674371-AMBULATORIO', 'M8', 1951.53 UNION ALL
      SELECT '20501781291-MATERNIDAD', 'M9', 3667.84 UNION ALL
      SELECT '20394674371-PREVENCION', 'M10', 853.68 UNION ALL
      SELECT '20454135432-EMERGENCIA', 'M11', 1449.85 UNION ALL
      SELECT '20546292658-EMERGENCIA', 'M12', 2172.90 UNION ALL
      SELECT '20100251176-EMERGENCIA', 'M13', 1538.13 UNION ALL
      SELECT '20102756364-PREVENCION', 'M14', 519.47 UNION ALL
      SELECT '20381170412-ONCOLOGIA', 'M15', 19334.91 UNION ALL
      SELECT '20381170412-AMBULATORIO', 'M16', 1286.69 UNION ALL
      SELECT '20501781291-ONCOLOGIA', 'M17', 11842.81 UNION ALL
      SELECT '20102756364-EMERGENCIA', 'M18', 1887.66 UNION ALL
      SELECT '20501781291-HOSPITALARIO', 'M19', 80000 UNION ALL
      SELECT '20454135432-MATERNIDAD', 'M20', 2603.76 UNION ALL
      SELECT '20454135432-PREVENCION', 'M21', 1872.20 UNION ALL
      SELECT '20546292658-ONCOLOGIA', 'M22', 4617.20 UNION ALL
      SELECT '20454135432-ONCOLOGIA', 'M23', 10573.66 UNION ALL
      SELECT '20501781291-PREVENCION', 'M24', 1194.13 UNION ALL
      SELECT '20546292658-MATERNIDAD', 'M25', 1956.74 UNION ALL
      SELECT '20454135432-HOSPITALARIO', 'M26', 29315.64 UNION ALL
      SELECT '20100251176-MATERNIDAD', 'M27', 2234.24 UNION ALL
      SELECT '20381170412-EMERGENCIA', 'M28', 2111.45 UNION ALL
      SELECT '20102756364-ONCOLOGIA', 'M29', 6035.15 UNION ALL
      SELECT '20102756364-MATERNIDAD', 'M30', 1375.09 UNION ALL
      SELECT '20546292658-PREVENCION', 'M31', 159.29 UNION ALL
      SELECT '20546292658-HOSPITALARIO', 'M32', 19866.88 UNION ALL
      SELECT '20100251176-HOSPITALARIO', 'M33', 36651.96 UNION ALL
      SELECT '20100251176-PREVENCION', 'M34', 55.00 UNION ALL
      SELECT '20102756364-HOSPITALARIO', 'M35', 29795.44 UNION ALL
      SELECT '20102756364-OTROS', 'M36', 50.00 UNION ALL
      SELECT '20381170412-PREVENCION', 'M37', 6115.63 UNION ALL
      SELECT '20394674371-ONCOLOGIA', 'M38', 2774.73

    ) AS m ON CONCAT(a.ruc_proveedor_emisor_ocr, '-', a.agrupacion_cobertura_negocio) = m.key
  ),

   --Reglas de duplicidad por similitudes en pdfs
  reglas_duplicidad_pago as
  (
      select a.*,
      b.num_siniestro as num_siniestro_factDup,
      b.factura_duplicada,
      b.monto_duplicado,
      b.fec_atencion,
      CASE WHEN b.flag_dup= 1  THEN 1 ELSE 0 END AS flag_duplicidad_pago,
      b.dsc_dup,b.flag_abono,b.sts_abono,b.dsc_abono, b.flag_observada, b.flag_devolucion,
      b.estado_procesado,
      from  InvoiceData_WithPlanCoincidenceFlag_Anomalies a
      left join  `{{project_id}}.siniestro_salud_auna.pdfs_auna_reglas` b
      on a.num_factura_documento_ocr=b.num_factura_documento_ocr
  ),

  reglas_autoseguros AS (
    SELECT 
      a.*,
      CASE 
        WHEN B.RUC IS NOT NULL AND REGEXP_CONTAINS(a.producto_siteds_mod, r'AMC|EPS|PLANES') THEN 1 ELSE 0 
      END AS flag_contratante_autoseguro
    FROM reglas_duplicidad_pago as a
    LEFT JOIN `{{project_id}}.siniestro_salud_auna.auna_listado_empresas_autoseguros` AS B
    ON UPPER(A.razon_social_contratante_siteds_ocr) = UPPER(B.razon_social)
  ),
  --Reglas de Nota de Crédito (solo es 1)
  reglas_notaCredito as
  (
      select 
        *, 
        CASE -- Con NC trama, pero sin NC en el sustento
          WHEN (flag_conNC_trama = 1 AND num_factura_notaCredito_ocr IS NULL) THEN 1 ELSE 0
        END AS flag_conNC_trama_sinNC_sustento,
        CASE -- Con NC Sustento, pero sin NC en la trama
          WHEN (flag_conNC_trama = 0 AND num_factura_notaCredito_ocr IS NOT NULL) THEN 1 ELSE 0
        END AS flag_sinNC_trama_conNC_sustento,
        CASE -- Con NC Sustento y con NC en la trama
          WHEN (flag_conNC_trama = 1 AND num_factura_notaCredito_ocr IS NOT NULL) THEN 1 ELSE 0
        END AS flag_conNC_trama_conNC_sustento,
        CASE -- Fecha emision de sustento y trama no coincide
          WHEN (fecha_nc_limpia <> fecha_nc_trama) THEN 1 ELSE 0
        END AS flag_fechaEmisionNC_noCoincide_sustento_trama,
        CASE -- El monto subtotal de la NC del sustento y trama no coincide
          WHEN (ROUND(subtotal_ocr_nc, 0) <> ROUND(montoSub_nc_trama, 0) ) THEN 1 ELSE 0
        END AS flag_subtotalNC_noCoincide_sustento_trama,
        CASE -- El monto total de la NC del sustento y RS no coincide
          WHEN (ROUND(importe_total_ocr_nc, 0) <> ROUND(mnt_nota_credito, 0) ) AND importe_total_ocr_nc <> 0 THEN 1 ELSE 0
        END AS flag_totalNC_noCoincide_RS,
        -- CASE -- Monto sub total de NC y Trama no coincide
        --   WHEN (montoSub_nc <> montoSub_nc_trama) THEN 1 ELSE 0
        -- END AS flag_fechaEmisionNC_noCoincide_sustento_trama,
        
      from reglas_autoseguros
  ),

  -- Reglas de Preliquidacion
  reglas_preLiquidacion AS
  (
    SELECT 
      *,
      CASE -- Fecha de Ingreso > Fecha alta (no debería pasar)
        WHEN ( DATE(fec_ingreso_preliqui_ocr) > DATE(fec_alta_preliqui_ocr) ) AND mecanismo_liqui_ocr <> 'CPM' THEN 1 ELSE 0
      END AS flag_fecIngreso_mayor_fecAlta_preliqui_ocr,
    FROM reglas_notaCredito
  ),

  -- Reglas de Liquidacion
  reglas_liquidacion AS
  (
    SELECT 
      *,
      CASE -- Fecha de Ingreso > Fecha alta (no debería pasar)
        WHEN ( DATE(fec_ingreso_liqui_ocr) > DATE(fec_alta_liqui_ocr) ) THEN 1 ELSE 0
      END AS flag_fecIngreso_mayor_fecAlta_liqui_ocr,
    FROM reglas_preLiquidacion
  ),

  -- Reglas de Epicrisis
  reglas_epicrisis AS
  (
    SELECT 
      *,
      -- Fecha de Egreso vs Ingreso mayor a días de hospitalización
      CASE 
        WHEN (DATE_DIFF(fec_egreso_epi_ocr, fec_ingreso_epi_ocr, DAY) > dias_hosp_epi_ocr) AND num_factura_epicrisis_ocr IS NOT NULL THEN 1 ELSE 0
      END AS flag_fechasHosp_NoCoincide_diasHosp_epi_ocr
    FROM reglas_liquidacion
  ),

  reglas_cross AS
  (
    SELECT 
      *, -- 1 es RIMACEPS y 2 Rimac Seguros
      CASE -- El RUC de Rimac en la factura no coincide con el producto usado en SITEDS (Rimac Seguros y Rimac EPS)
        WHEN (flag_producto_eps_siteds_ocr = 1 AND flag_compania_factura_ocr <> 1 AND producto_siteds_ocr IS NOT NULL AND ruc_compania_factura_ocr IS NOT NULL) THEN 1
        WHEN (flag_producto_eps_siteds_ocr = 0 AND flag_compania_factura_ocr = 1 AND producto_siteds_ocr IS NOT NULL AND ruc_compania_factura_ocr IS NOT NULL) THEN 1
        ELSE 0
      END AS flag_rucRimac_Fact_NoCoincide_producto_Siteds_ocr,
      CASE -- El Monto Fact OCR coincide con Monto Preliquidacion OCR
        WHEN (monto_factura_ocr <> monto_total_preliqui_ocr AND num_factura_preliqui_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_montoTotal_NoCoincide_Fact_Preli_ocr,
      CASE -- Fecha de ocurrencia SITEDS coincide con Fecha de ingreso Preliquidacion (debería coincidir)
        WHEN (fecha_emision_siteds_ocr <> DATE(fec_ingreso_preliqui_ocr) AND num_factura_preliqui_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_fecAutoSiteds_ocr_NoCoincide_fecIngreso_preliqui_ocr,
      CASE -- El Monto Fact OCR coincide con Monto Liquidacion OCR (margen de 0.1 favoreciendo Liquidacion) -- 
        WHEN ( (monto_total_liqui_ocr - monto_factura_ocr) > 0.1 AND num_factura_liqui_ocr IS NOT NULL AND monto_total_liqui_ocr <> 0) THEN 1 ELSE 0
      END AS flag_montoTotal_NoCoincide_Fact_Liqui_ocr,
      -- CASE -- Fecha de ocurrencia SITEDS coincide con Fecha de ingreso liquidacion (debería coincidir)
      --   WHEN (fecha_emision_siteds_ocr <> DATE(fec_ingreso_liqui_ocr) AND num_factura_liqui_ocr IS NOT NULL) THEN 1 ELSE 0
      -- END AS flag_fecAutoSiteds_ocr_NoCoincide_fecIngreso_liqui_ocr,

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

      CASE -- DNI de la hoja SITEDS coincide con DNI de RS
        WHEN (num_documento_paciente_siteds_ocr_val <> num_documento_afiliado AND num_factura_liqui_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_dniPaciente_Siteds_ocr_NoCoincide_RS,
      
      CASE -- Cod Autorizacion de la hoja SITEDS coincide con cod autorizacion de la hoja LIQUIDACION (Cuando es CPM que no se alerte)
        WHEN (num_siteds_ocr_val <> cod_autorizacion_liqui_ocr AND num_factura_liqui_ocr IS NOT NULL AND mecanismo_liqui_ocr <> 'CPM') THEN 1 ELSE 0
      END AS flag_codAutorizacion_Siteds_ocr_NoCoincide_Liqui_ocr,
      CASE -- El % del copago variable cubierto del SITEDS no coincide con el monto coaseguro del paciente en Liquidacion (Sacando el % al subtotal2 DEBERÍA el monto del coaseguro del paciente) pct_coaseguro_liqui_ocr
        -- WHEN (
        --   ROUND((1.0 - cobertura_copago_variable_siteds_ocr) * gastos_afectos_subtotal2_liqui_ocr) <> ROUND(gastos_afectos_coaseguroPaciente_liqui_ocr) 
        --   AND num_factura_liqui_ocr IS NOT NULL) THEN 1 ELSE 0
      -- END AS flag_copagoPaciente_Siteds_ocr_NoCoincide_coaseguroPaciente_Liqui_ocr,
        WHEN (
          ROUND(1.0 - cobertura_copago_variable_siteds_ocr) <> ROUND(pct_coaseguro_liqui_ocr/100) 
          AND num_factura_liqui_ocr IS NOT NULL AND cobertura_copago_variable_siteds_ocr IS NOT NULL AND pct_coaseguro_liqui_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_pctCopagoPaciente_Siteds_ocr_NoCoincide_pctCoaseguroPaciente_Liqui_ocr,
      CASE -- El subtotal de Liquidacion no coincide con Trama
        WHEN (ROUND(gastos_afectos_subtotal3_liqui_ocr) <> ROUND(subtotal_fact_trama) AND num_factura_liqui_ocr IS NOT NULL AND gastos_afectos_subtotal3_liqui_ocr <> 0) THEN 1 ELSE 0
      END AS flag_subtotal_Liqui_ocr_NoCoincide_Fact_trama,
      CASE -- El copago de SITEDS coincide con el deducible de LIQUIDAION
        WHEN (copago_fijo_siteds_ocr <> deducible_liqui_ocr AND num_factura_liqui_ocr IS NOT NULL --AND cobertura_siteds_ocr_limpia = 'AMBULATORIO' 
        AND flag_consultaAmboMed_liqui_ocr = 1
        AND mecanismo_liqui_ocr = 'PPS') THEN 1 ELSE 0
      END AS flag_deduciblePaciente_Siteds_ocr_NoCoincide_Liqui_ocr,
      -- El mecanismo de pago de Liquidacion NO coincide con RS (Rimac Salud)
      CASE
        WHEN 
          (mecanismo_liqui_ocr = 'CPM' AND cod_mecanismo_pago != '02') OR
          (mecanismo_liqui_ocr = 'PQ' AND cod_mecanismo_pago != '03') OR
          (mecanismo_liqui_ocr = 'PPS' AND cod_mecanismo_pago != '01')
        THEN 1 ELSE 0
      END AS flag_mecanismoPago_Liquidacion_ocr_NoCoincide_RS,
      -- El mecanismo de pago de Liquidacion NO coincide con Trama
      CASE
        WHEN 
          (mecanismo_liqui_ocr = 'CPM' AND mecanismo_pago_fact_trama != '02') OR
          (mecanismo_liqui_ocr = 'PQ' AND mecanismo_pago_fact_trama != '03') OR
          (mecanismo_liqui_ocr = 'PPS' AND mecanismo_pago_fact_trama != '01')
        THEN 1 ELSE 0
      END AS flag_mecanismoPago_Liquidacion_ocr_NoCoincide_trama,
      CASE -- Razon social de Factura coincide con Razon social de la hoja Carta Garantia
        WHEN (razon_social_fact_limpia_ocr <> razon_social_proveedor_cg_limpia_ocr AND num_factura_cartagarantia_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_razonSocial_fact_ocr_NoCoincide_cg_ocr,
      CASE -- RUC de Factura coincide con RUC de la hoja Carta Garantia
        WHEN (ruc_proveedor_emisor_ocr <> ruc_proveedor_cg_ocr AND num_factura_cartagarantia_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_ruc_fact_ocr_NoCoincide_cg_ocr,
      -- CASE -- La CG de la trama está asociada a más de un siniestro
      --   WHEN (flag_siniestroDoble = 1 AND flag_conCG_trama = 1) THEN 1 ELSE 0
      -- END AS flag_cg_dobleSiniestro,
      NULL AS flag_cg_dobleSiniestro,
      -- CASE -- La CG de la trama está asociada a más de una factura
      --   WHEN (flag_factura_conocida = 1 AND flag_conCG_trama = 1) THEN 1 ELSE 0
      -- END AS flag_cg_dobleFactura,
      CASE -- La fecha de Ingreso de Epicrisis menor a fecha de autorización de siteds 
        WHEN (fec_ingreso_epi_ocr < fecha_emision_siteds_ocr AND num_factura_epicrisis_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_fecIngreso_epi_ocr_menor_fecSiteds_ocr,
      CASE -- El DX de SITEDS no coincide con DX de Trama Siteds
        WHEN (cie10_siteds_ocr = cod_cie101_trama_sited OR cie10_siteds_ocr = cod_cie102_trama_sited OR cie10_siteds_ocr = cod_cie103_trama_sited) THEN 0
        WHEN cie10_siteds_ocr IS NULL OR cod_cie101_trama_sited IS NULL THEN 0
        ELSE 1
      END AS flag_dxSiteds_ocr_NoCoincide_trama,
      CASE -- La cobertura de SITEDS no coincide con cobertura de Trama Siteds
        WHEN (cobertura_siteds_ocr_limpia <> cobertura_siteds_trama AND cobertura_siteds_ocr_limpia IS NOT NULL AND cobertura_siteds_trama IS NOT NULL) THEN 1 ELSE 0
      END AS flag_coberturaSiteds_ocr_NoCoincide_trama,
      CASE -- Es un ambulatorio con medicamentos pero sin Receta Medica
        WHEN (cobertura_siteds_ocr_limpia = 'AMBULATORIO' AND flag_medicamento_factura_ocr = 1 AND num_factura_rc_ocr IS NULL AND flag_consultaAmboMed_liqui_ocr = 1) THEN 1 ELSE 0
      END AS flag_sinReceta_ambulatorio, -- 
      CASE -- Para casos hospitalarios el diagnostico de entrada de CG coincide con Diagnostico de entrada de Epicrisis
        WHEN (cobertura_siteds_ocr_limpia = 'OTROS' AND dx_entrada_transformada_cg_ocr <> dx_ingreso_epi_ocr AND num_factura_epicrisis_ocr IS NOT NULL) THEN 1 ELSE 0
      END AS flag_dxEntrada_CG_NoCoincide_dxEntrada_epi,
      CASE -- Empresa con Autoseguro ha pasado los 12 meses de vencimiento
        WHEN (DATE_DIFF(CURRENT_DATE(), fecha_emision_siteds_ocr, YEAR) >= 1 AND flag_contratante_autoseguro = 1) THEN 1 ELSE 0
      END AS flag_autoseguro_vencido,

      CASE -- La fecha de Siteds debe ser antes de fecha de Factura
        WHEN (fecha_emision_siteds_ocr > fecha_emision_fact_limpio_val AND num_siteds_ocr_val IS NOT NULL) THEN 1 ELSE 0
      END AS flag_fecFact_mayor_fecSiteds_ocr,

      CASE -- El monto total de Factura no coincide con Trama
        WHEN (ROUND(monto_factura_ocr) <> ROUND(monto_factura_trama) AND monto_factura_ocr IS NOT NULL AND monto_factura_trama IS NOT NULL) THEN 1 ELSE 0
      END AS flag_MtoTotal_ocr_NoCoincide_Trama,

      CASE -- La fecha de atencion no esta dentro del rango de CG
        WHEN num_factura_cartagarantia_ocr IS NOT NULL 
       AND (
        fecha_emision_siteds_ocr < fec_emision_cg_ocr OR 
        fecha_emision_siteds_ocr > fec_validez_cg_ocr
        ) THEN 1 ELSE 0
      END AS flag_fecAtencion_NoCoincide_rango_cg,

      CASE -- El afiliado no esta vigente al momento de la atencion siteds
        WHEN (fecha_emision_siteds_ocr NOT BETWEEN fec_inicio_vigencia AND fec_fin_vigencia) AND fec_fin_vigencia IS NOT NULL AND fecha_emision_siteds_ocr IS NOT NULL THEN 1 ELSE 0
      END AS flag_afiliadoNoVigenteAtencion,

      CASE -- Afiliado no activo dentro del rango de CG
        WHEN num_factura_cartagarantia_ocr IS NOT NULL 
       AND 
       (fec_emision_cg_ocr NOT BETWEEN fec_inicio_vigencia AND fec_fin_vigencia) OR 
       (fec_validez_cg_ocr NOT BETWEEN fec_inicio_vigencia AND fec_fin_vigencia) 
       THEN 1 ELSE 0
      END AS flag_afiliado_NoVigente_rango_cg,

    FROM reglas_epicrisis
  ),

  reglas_cartaGarantia AS
  (
    
    select 
        *, 
        CASE -- Con CG trama, pero sin CG en el sustento
          WHEN (flag_conCG_trama = 1 AND num_factura_cartagarantia_ocr IS NULL) THEN 1 ELSE 0
        END AS flag_conCG_trama_sinCG_sustento,
        CASE -- Con CG Sustento, pero sin CG en la trama
          WHEN (flag_conCG_trama = 0 AND num_factura_cartagarantia_ocr IS NOT NULL) THEN 1 ELSE 0
        END AS flag_sinCG_trama_conCG_sustento,
        CASE -- Con CG Sustento y con CG en la trama
          WHEN (flag_conCG_trama = 1 AND num_factura_cartagarantia_ocr IS NOT NULL) THEN 1 ELSE 0
        END AS flag_conCG_trama_conCG_sustento,
        CASE -- Monto facturación mayor a monto final de CG (hay cierta tolerancia para 2 coberturas)
          WHEN (monto_factura_ocr > (monto_final_cg + 1000) AND agrupacion_cobertura_negocio = 'AMBULATORIO' ) THEN 1 
          WHEN (monto_factura_ocr > (monto_final_cg + 2000) AND agrupacion_cobertura_negocio = 'HOSPITALARIO' ) THEN 1
          WHEN (monto_factura_ocr > monto_final_cg AND agrupacion_cobertura_negocio NOT IN ('AMBULATORIO', 'HOSPITALARIO') ) THEN 1
          ELSE 0
        END AS flag_monto_excede_cg,
        -- CASE -- CG asociada a más de una factura
        --   WHEN (nro_carta > num_carta_garantia) THEN 1 ELSE 0
        -- END AS flag_monto_excede_cg, 
        
      from reglas_cross
  ),
  -- InvoiceData_DedupedByPlanCoincidence: Elimina duplicados de facturas, priorizando aquellas con
  -- coincidencia de plan para asegurar la relevancia de los datos.
  
  InvoiceData_DedupedByPlanCoincidence AS (
    SELECT
      a.*
    FROM
      reglas_cartaGarantia AS a
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY num_factura_documento_ocr ORDER BY coincide_plan DESC) = 1
  )
 select *
 from  InvoiceData_DedupedByPlanCoincidence
;







------------- BACKUP RULES
-- CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas_backup_20250915`
-- AS
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas`
-- ;