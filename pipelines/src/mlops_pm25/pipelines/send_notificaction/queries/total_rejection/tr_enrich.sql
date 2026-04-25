#################################################################
############# 10_FAB_SS_Reglas_Recupero_Proveedores #############
#################################################################

# 1. Objetivo: Crear reglas de recupero no solo para AUNA, sino para todos los proveedores pero netamente usando la trama, porque el OCR se va a demorar en crearse para todos los proveedores

# 2. Subojetivos: 
### 2.1. Mapear las tablas a usar
### 2.2. Mapear qué campos vamos a usar
### 2.3. Comparar campos del ocr vs trama (para ver si es factible)
#################################################################

DECLARE PERIODO_INI STRING;


SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

######################################################################################################
###################################### 1. Trama Factura ##############################################
######################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.factura_trama`
  AS
  SELECT
  
  NUMERODOCUMENTOPAGO AS num_factura_documento_ocr,
  RUCIPRESS AS ruc_proveedor_emisor_ocr,
  CODIGOFINANCIADOR AS ruc_compania_factura_ocr,
  -- NUMERODOCUMENTOIDENTIDAD AS dni_factura_ocr, # Trama siteds
  FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', FECHAEMISION)) AS fecha_emision_fact_limpio_val,
  # flag_compania_factura_ocr # Trama factura, esto se crea con un CASE
  # numero_del_documento_de_autorizacion AS cod_autorizacion_factura_ocr # Trama siteds
  SAFE_CAST(MONTONETO AS FLOAT64) AS monto_sub_factura_ocr,
  SAFE_CAST(MONTOTOTAL AS FLOAT64) AS monto_factura_ocr,
  ------ 
  CODIGOPRODUCTO AS codigoproducto_trama, # Trama siteds # Esto no es necesario al parecer
  CODIGOMECANISMOPAGO AS mecanismo_liqui_ocr, # Trama factura
  FECHAENVIO, --AS processed_date, # Este no es el dato, es temporal
  NUMEROLOTE,
  CODIGOLOTE
  FROM `{{project_id}}.siniestro_salud_auna.SEPS_DOCUMENTO_FACTURADOR_M`
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY RUCIPRESS,NUMERODOCUMENTOPAGO ORDER BY CODIGOLOTE DESC) = 1
  --WHERE FECHAENVIO >= '20250101'
;




-- SELECT CODIGOMECANISMOPAGO, COUNT(*) FROM `{{project_id}}.siniestro_salud_auna.SEPS_DOCUMENTO_FACTURADOR_M` GROUP BY ALL ORDER BY 2 DESC

######################################################################################################
###################################### 2. Trama Siteds ###############################################
######################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.siteds_trama`
  AS
  SELECT
  
  NUMERODOCUMENTOPAGO AS num_factura_documento_ocr,
  RUCIPRESS AS ruc_proveedor_emisor_ocr,
  NUMERODOCUMENTOIDENTIDAD AS dni_factura_ocr, # Trama siteds
  NUMEROAUTORIZACION AS num_siteds_ocr_val,
  TIPOCOBERTURA AS cobertura_siteds_trama,
  NUMERODOCUMENTOIDENTIDAD AS num_documento_paciente_siteds_ocr_val, # De dónde saco el dni del paciente
  FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', FECHAINICIOATENCION)) AS fecha_emision_siteds_ocr,
  # AS num_poliza_siteds_ocr, ## SITEDS # # De dónde saco el num poliza usado
  CODIGOCIE101 AS cie10_siteds_ocr, --dx ## SITEDS
FROM `{{project_id}}.siniestro_salud_auna.SEPS_TEMP_ATENCION_M`
;





######################################################################################################
#################################### 3. Consolidado de Trama #########################################
######################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_siteds`
AS
SELECT
  A.*,
  B.* EXCEPT (num_factura_documento_ocr, ruc_proveedor_emisor_ocr)
FROM `{{project_id}}.siniestro_salud_auna.factura_trama` AS A
LEFT JOIN `{{project_id}}.siniestro_salud_auna.siteds_trama` AS B
ON A.num_factura_documento_ocr = B.num_factura_documento_ocr AND A.ruc_proveedor_emisor_ocr = B.ruc_proveedor_emisor_ocr
;




######################################################################################################
#################################### 4. Universo de siniestros #######################################
######################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.universo_siniestros_base`
AS
SELECT 
  DISTINCT
  PERIODO_INI AS processed_date, -- Poner el límite superior del fec_notificacion pero sin las horas
  num_documento_proveedor_siniestro,
  --concat(cod_sede_proveedor_siniestro,num_comprobante_siniestro)
  CONCAT(cod_serie_comprobante_siniestro, LPAD(num_comprobante_siniestro, 8, '0')) AS numero_factura
FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud` A
WHERE TRUE
  AND periodo  = DATE_TRUNC(current_date(),month) 
  AND tip_reclamo='C' --solo credito
  
  -- Si hoy es miercoles - sabado: 
  AND fec_notificacion BETWEEN "{{lower_limit_timestamp}} 12:00:00 UTC" AND "{{upper_limit_timestamp}} 12:00:00 UTC" -- PONER DOS DIAS ANTES COMO LIMITE INFERIOR Y UNO COMO LIMITE SUPERIOR
  -- Si hoy es martes
  -- AND fec_notificacion BETWEEN "2025-11-15 12:00:00 UTC" AND "2025-11-17 12:00:00 UTC" -- PONER CUATRO DIAS ANTES COMO LIMITE INFERIOR Y UNO COMO LIMITE SUPERIOR

  AND tip_caso_especial is null -- solo flujo regular (valor null o CONCILIACIONES )
  AND SUBSTR(id_siniestro, 4, 1) <> '9'
  AND num_documento_proveedor_siniestro IN ( 
    ---- AUNA ----
    '20546292658',
    '20501781291',
    '20454135432',
    '20394674371',
    '20381170412',
    '20102756364',
    '20100251176',
    ----- CI -----
    '20100054184',
    -- Anglo American --
    '20107695584',
    -- Ricardo Palma --
    '20100121809',
    -- San Pablo --
    '20544206410',
    '20517737560',
    '20107463705',
    '20517738701',
    '20505018509',
    '20508790971',
    '20502454111',
    '20601725551',
    --- Sanna ---
    '20136096592',
    '20507264108',
    '20507775889',
    '20112280201',
    '20100162742'
  )
GROUP BY ALL
;



######################################################################################################
#################################### 5. Universo Final (cruce) #######################################
######################################################################################################
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.universo_final_facturas_trama`
AS
SELECT 
  DISTINCT
  A.*,
  B.processed_date, 
FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds` AS A
INNER JOIN `{{project_id}}.siniestro_salud_auna.universo_siniestros_base` AS B
ON A.num_factura_documento_ocr = B.numero_factura AND A.ruc_proveedor_emisor_ocr = B.num_documento_proveedor_siniestro
;

-- SELECT COUNT(DISTINCT num_factura_documento_ocr) FROM `{{project_id}}.siniestro_salud_auna.universo_final_facturas_trama`


######################################################################################################
#################################### 6. Enrichment ###################################################
######################################################################################################


########### 6.1. Enrichment Siniestro_Detalle_salud
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v1`
AS
WITH 
ctdSitedsRS AS(
  SELECT
    concat(cod_serie_comprobante_siniestro,lpad(num_comprobante_siniestro,8,'0')) as factura,
    num_documento_proveedor_siniestro,
    COUNT(DISTINCT id_autorizacion) as ctdAtencionesSitedsxFacturaRS
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud`
  WHERE TRUE
  AND periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
  GROUP BY ALL
)
SELECT 
  A.* EXCEPT(NUMEROLOTE),
  B.id_persona_afiliado,
  B.num_siniestro,
  B.id_siniestro,
  B.id_poliza,
  B.num_poliza as num_poliza_siteds_ocr, 
  ##### CAMPOS NUEVOS INCLUIDOS #####
  B.des_producto, -- campo nuevo
  B.cod_producto_origen, -- campo nuevo
  B.cod_tipo_contrato, -- campo nuevo
  CASE WHEN B.cod_tipo_contrato IN (
    6,156,191,189,154,4,3,153,188,322
  ) THEN B.cod_tipo_contrato ELSE 9999 END AS cod_tipo_contrato_cronico,
  B.des_cobertura,
  ####################################
  A.NUMEROLOTE AS num_lote,
  B.num_documento_contratante AS ruc_contratante,
  B.fec_notificacion,
  B.id_autorizacion,
  B.cod_sede_proveedor_siniestro,
  B.nom_sede_proveedor_siniestro,
  C.ctdAtencionesSitedsxFacturaRS,
  -- Tipo de contrato Cronicos
  CASE 
    WHEN cod_tipo_contrato IN (154, 189, 4) THEN 'PACIENTE MES DIABETES'
    WHEN cod_tipo_contrato IN (156, 191, 6) THEN 'PACIENTE MES DISLIPIDEMIA'
    WHEN cod_tipo_contrato IN (153, 188, 3) THEN 'PACIENTE MES ASMA'
    WHEN cod_tipo_contrato IN (322) THEN 'PACIENTE MES ANEMIA'
    ELSE 'OTROS'
  END AS contrato_cronico_texto,
  -- Grupo de clínicas
  CASE
    WHEN B.num_documento_proveedor_siniestro IN (
      '20102756364', '20394674371', '20100251176', '20454135432', '20546292658', '20381170412', '20501781291'
    ) THEN 'AUNA'
    WHEN B.num_documento_proveedor_siniestro IN (
      '20100054184'
    ) THEN 'CI'
    WHEN B.num_documento_proveedor_siniestro IN (
      '20107695584'
    ) THEN 'AngloAmerican' 
    WHEN B.num_documento_proveedor_siniestro IN (
      '20100121809'
    ) THEN 'Ricardo Palma' 
    WHEN B.num_documento_proveedor_siniestro IN (
      '20544206410', '20517737560', '20107463705', '20517738701', '20505018509', '20508790971', '20502454111', '20601725551'
    ) THEN 'San Pablo'
    WHEN B.num_documento_proveedor_siniestro IN (
      '20136096592', '20507264108', '20507775889', '20112280201', '20100162742'
    ) THEN 'Sanna'
  END AS grupo_clinica_texto
FROM `{{project_id}}.siniestro_salud_auna.universo_final_facturas_trama` AS A
LEFT JOIN (
  SELECT * FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud`
  LEFT JOIN UNNEST(atencion_salud)
  WHERE TRUE
  AND periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
) AS B
ON A.num_factura_documento_ocr = concat(B.cod_serie_comprobante_siniestro,lpad(B.num_comprobante_siniestro,8,'0')) AND A.ruc_proveedor_emisor_ocr = B.num_documento_proveedor_siniestro
LEFT JOIN ctdSitedsRS AS C
ON A.num_factura_documento_ocr = C.factura AND A.ruc_proveedor_emisor_ocr = C.num_documento_proveedor_siniestro
;


-- SELECT  FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v1`

########### 6.2. Enrichment Autoseguros
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v2`
AS
SELECT 
  A.*,
  CASE 
    WHEN B.RUC IS NOT NULL AND A.codigoproducto_trama IN (
      '1', '62', '59', -- AMC
      'S', -- EPS
      '4', '11', '12', '38', '50', '52', '53', '95', 'E1', '7', '49', '54', '93', '2', '15', '28', '34', '16', '20' -- AMI
    ) THEN 1 ELSE 0
  END AS flag_contratante_autoseguro,
FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v1` AS A
LEFT JOIN `{{project_id}}.siniestro_salud_auna.auna_listado_empresas_autoseguros` AS B
ON A.ruc_contratante = B.RUC
;



####################### 6.3.1 Tabla de Unidad Asegurable + cod_producto_origen
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.unidad_asegurable_productos_temporal` AS
  SELECT 
    DISTINCT
    A.id_persona, 
    A.id_unidad_asegurable, 
    A.fec_inicio_vigencia, 
    A.fec_fin_vigencia, 
    A.des_estado_origen_unidad_asegurable,
    REGEXP_EXTRACT(A.id_unidad_asegurable, r'^(.*)-[^-]+-[^-]+$') AS id_poliza, 
    A.id_certificado,
    B.cod_producto_origen,
    B.des_producto,
    B.des_producto_agrupado,
    ROW_NUMBER() OVER (
      PARTITION BY A.id_persona, REGEXP_EXTRACT(A.id_unidad_asegurable, r'^(.*)-[^-]+-[^-]+$')
      ORDER BY 
        CASE WHEN A.des_estado_origen_unidad_asegurable = 'VIGENTE' THEN 1 ELSE 2 END,
        A.fec_inicio_vigencia DESC
    ) AS rn
  FROM `{{project_id}}.tmp.unidad_asegurable` AS A
  LEFT JOIN (
    SELECT * FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.siniestro_detalle_salud`
    WHERE TRUE
    AND periodo  = DATE_TRUNC(current_date(),month) 
    AND tip_reclamo='C' --solo credito
  ) AS B
  ON A.id_persona = B.id_persona_afiliado
  AND REGEXP_EXTRACT(A.id_unidad_asegurable, r'^(.*)-[^-]+-[^-]+$') = B.id_poliza
  WHERE A.id_origen = 'RS'
  ;


########### 6.3.2 Enrichment Unidad Asegurable
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v3`
AS

SELECT 
  DISTINCT
  A.*,
  -- Rango de afiliado activo (no cg)
  B.fec_inicio_vigencia,
  B.fec_fin_vigencia,
  --B.des_producto,
  --B.cod_producto_origen
FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v2` AS A
LEFT JOIN (
  SELECT 
    DISTINCT
    A.id_persona, 
    A.id_unidad_asegurable, 
    A.fec_inicio_vigencia, 
    A.fec_fin_vigencia, 
    A.des_estado_origen_unidad_asegurable,
    A.id_poliza, 
    A.id_certificado,
    A.cod_producto_origen,
    --A.des_producto,
    A.des_producto_agrupado,
    ROW_NUMBER() OVER (
      PARTITION BY A.id_persona, REGEXP_EXTRACT(A.id_unidad_asegurable, r'^(.*)-[^-]+-[^-]+$')
      ORDER BY 
        CASE WHEN A.des_estado_origen_unidad_asegurable = 'VIGENTE' THEN 1 ELSE 2 END,
        A.fec_inicio_vigencia DESC
    ) AS rn
  FROM `{{project_id}}.siniestro_salud_auna.unidad_asegurable_productos_temporal` AS A
) AS B
ON A.id_persona_afiliado = B.id_persona 
AND A.id_poliza = REGEXP_EXTRACT(id_unidad_asegurable, r'^(.*)-[^-]+-[^-]+$')
AND A.cod_producto_origen = B.cod_producto_origen
--WHERE A.id_persona_afiliado = 'AX-8242469'
;

-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v3`

########### 6.4. Enrichment Carta Garantia Solicitud
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v4`
AS
SELECT 
  A.*,
  -- Campos de CG
  B.num_carta_garantia,
  B.fec_aprobacion_carta,
  B.fec_validez_carta,
  B.mnt_carta_garantia_total,
  B.flag_cg_anulada,
  SAFE_CAST(SPLIT(CAST(B.num_carta_garantia AS STRING), '-')[OFFSET(0)] AS INT64) AS nro_carta
FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v3` AS A
LEFT JOIN (
  SELECT 
      periodo,
      num_carta_garantia,
      num_carta_garantia_origen,
      num_documento_proveedor,
      mnt_carta_garantia_total,
      id_siniestro,
      MAX(cgv.fec_aprobacion) AS fec_aprobacion_carta,
      MAX(DATE_ADD(cgv.fec_aprobacion, INTERVAL 30 DAY)) AS fec_validez_carta,
      CASE WHEN des_est_carta_garantia_origen IN ('ANULADA', 'AN', 'RECHAZADA') THEN 1 ELSE 0 END AS flag_cg_anulada
    FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.carta_garantia_solicitud`
    LEFT JOIN UNNEST(carta_garantia_version) as cgv
    WHERE periodo = DATE_TRUNC(CURRENT_DATE(), MONTH)
    GROUP BY ALL
) AS B
ON A.id_siniestro = B.id_siniestro
;





########### 6.5. Enrichment Autorizaciones Salud (Siteds)
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v5` 
AS
# El problema con autorizaciones_salud es que no tiene factura ni ruc, solo la llave id_autorizacion que vendría a ser practicamente el siteds
WITH ctdSitedsxLlave AS(
  SELECT 
    periodo_autorizacion,
    num_doc_proveedor,
    cod_sede_proveedor,
    --nom_sede_proveedor,
    id_persona_afiliado,
    ######## CAMPOS NUEVOS ########
    des_cobertura,
    des_producto,
    ###############################
    COUNT(DISTINCT cod_autorizacion) as ctdAtencionesSitedsxLlaveRS
  FROM `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.autorizaciones_salud`
  WHERE TRUE
  AND cod_autorizacion IN ( -- Que solo sean CPM
    SELECT DISTINCT num_siteds_ocr_val FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds`
    WHERE TRUE
    AND mecanismo_liqui_ocr = '02'
  )
  GROUP BY ALL
)
SELECT 
  A.*,
  B.cod_autorizacion,
  C.ctdAtencionesSitedsxLlaveRS
FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v4` AS A
LEFT JOIN `rs-shr-al-analyticsz-prj-ebc1.anl_siniestro.autorizaciones_salud` AS B 
ON A.num_siteds_ocr_val = B.cod_autorizacion
LEFT JOIN ctdSitedsxLlave AS C
ON (
  DATE_TRUNC(PARSE_DATE('%Y-%m-%d', A.fecha_emision_siteds_ocr), MONTH) = C.periodo_autorizacion
  AND A.ruc_proveedor_emisor_ocr = C.num_doc_proveedor
  AND A.cod_sede_proveedor_siniestro = C.cod_sede_proveedor
  AND A.id_persona_afiliado = C.id_persona_afiliado
  ############# NUEVO #############
  AND A.des_cobertura = C.des_cobertura
  AND A.des_producto = C.des_producto
  #################################
)
;






########### 6.6. Enrichment Agrupaciones itself
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v6` AS
WITH 
ctdSitedsxFacturaRucTrama AS(
  SELECT 
    num_factura_documento_ocr,
    ruc_proveedor_emisor_ocr,
    COUNT(DISTINCT num_siteds_ocr_val) AS ctdAtencionesSitedsxFacturaTrama
  FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v5`
  GROUP BY ALL
),
atencionesContratoClinicaPaciente AS
(
  SELECT 
    -- Ahora es por ruc, pero debe ser por nombre comercial o grupo de clinica
    FORMAT_DATE('%Y-%m-01', PARSE_DATE('%Y-%m-%d', fecha_emision_siteds_ocr)) AS fecha_normalizada,
    grupo_clinica_texto,
    cod_tipo_contrato_cronico,
    --contrato_cronico_texto,
    id_persona_afiliado,
    CASE -- El valor 9999 significa que no es CPM Cronico, entonces no se contaria
      WHEN cod_tipo_contrato_cronico <> 9999 THEN COUNT(DISTINCT num_siteds_ocr_val) ELSE 0
    END AS ctdAtencionesCPMCronicoClinica
  FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v5`
  GROUP BY ALL
)
SELECT 
  A.*,
  B.ctdAtencionesCPMCronicoClinica,
  C.ctdAtencionesSitedsxFacturaTrama
FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v5` AS A
LEFT JOIN atencionesContratoClinicaPaciente AS B
ON (
  FORMAT_DATE('%Y-%m-01', PARSE_DATE('%Y-%m-%d', A.fecha_emision_siteds_ocr)) = B.fecha_normalizada
  AND A.grupo_clinica_texto = B.grupo_clinica_texto
  AND A.cod_tipo_contrato_cronico = B.cod_tipo_contrato_cronico
  AND A.id_persona_afiliado = B.id_persona_afiliado
)
LEFT JOIN ctdSitedsxFacturaRucTrama AS C
ON A.num_factura_documento_ocr = C.num_factura_documento_ocr AND A.ruc_proveedor_emisor_ocr = C.ruc_proveedor_emisor_ocr
;






######################################################################################################
###################################### 6.7. Tabla Pre Enriched #########################################
######################################################################################################
-- Esta tabla se crea para usarse en el query de duplicity  
CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_duplicity`
AS
SELECT 
  DISTINCT
  *
FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched_v6`
;




