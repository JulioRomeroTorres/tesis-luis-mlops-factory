######################################################################################################
###################################### REGLAS ########################################################
######################################################################################################
-- Eliminar por si acaso
DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);

DELETE FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_reglas`
WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
;


INSERT INTO `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_reglas` 
--CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_reglas` AS
SELECT
  *,
  -- Empresa con Autoseguro ha pasado los 12 meses de vencimiento
  CASE 
    WHEN ruc_proveedor_emisor_ocr = '20100054184' # CI
    AND ruc_contratante IN ( ### TELEFONICA
    '20423924137',
    '20100070970',
    '20501827623',
    '20607092851',
    '20606862556'
    ) AND (DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y-%m-%d', fecha_emision_fact_limpio_val), MONTH) >= 6 AND flag_contratante_autoseguro = 1) THEN 1 
    WHEN (DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y-%m-%d', fecha_emision_fact_limpio_val), YEAR) >= 1 AND flag_contratante_autoseguro = 1) THEN 1 
    ELSE 0
  END AS flag_autoseguro_vencido,
  -- Afiliado no vigente entre las fechas de CG
  CASE
    WHEN num_carta_garantia IS NOT NULL AND (
        (fec_aprobacion_carta NOT BETWEEN fec_inicio_vigencia AND fec_fin_vigencia) 
        OR 
        (fec_validez_carta NOT BETWEEN fec_inicio_vigencia AND fec_fin_vigencia AND PARSE_DATE('%Y-%m-%d', fecha_emision_siteds_ocr) > fec_validez_carta)
    )
    THEN 1 ELSE 0
  END AS flag_afiliado_NoVigente_rango_cg,
  -- Duplicidad de Pago
  CASE 
    WHEN flag_dup = 1 THEN 1 ELSE 0 
  END AS flag_duplicidad_pago,
  -- La cantidad de siteds de CPM en la trama no coincide con la cantidad de siteds en la tabla autorizaciones
  -- CASE 
  --   WHEN cod_autorizacion IS NULL AND mecanismo_liqui_ocr = '02' AND num_siteds_ocr_val IS NOT NULL THEN 1 ELSE 0 
  -- END AS flag_ctdSiteds_trama_autorizaciones_noCoincide1,
  -- La cantidad de siteds de CPM en la trama no coincide con la cantidad de siteds en la tabla autorizaciones
  CASE 
    WHEN ctdAtencionesSitedsxLlaveRS > ctdAtencionesSitedsxFacturaTrama AND mecanismo_liqui_ocr = '02' THEN 1 ELSE 0 
  END AS flag_ctdSiteds_trama_autorizaciones_noCoincide,
  -- CPM cronico con mas de una atencion en un mismo grupo de clinica
  CASE 
    WHEN ctdAtencionesCPMCronicoClinica > 1 --AND contrato_cronico_texto <> 'OTROS' 
    AND id_persona_afiliado IS NOT NULL THEN 1 ELSE 0 
  END AS flag_afiliadoCPMAtencionesGrupoClinica
FROM `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_enriched`
;





-- #############################################################################
-- #                       REGLAS INTELIGENTES FACTURACION
-- #############################################################################



####################################
######### Fuentes Usadas ###########
####################################
-- 1. `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas` -- Esto fue creado en Enrich_for_rules
######################################


DELETE FROM `{{project_id}}.siniestro_salud_auna.base_reglas_finales_proveedores`
WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
;


INSERT INTO
  `{{project_id}}.siniestro_salud_auna.base_reglas_finales_proveedores`

--CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.base_reglas_finales_proveedores` AS
WITH FacturaDatos AS (
    -- CTE 1: Define tu tabla de origen y selecciona las columnas necesarias.
    -- Reemplaza 'your_project.your_dataset.your_table' con la ruta completa a tu tabla en BigQuery.
    SELECT
        processed_date,
        grupo_clinica_texto,
        fecha_emision_siteds_ocr,
        num_factura_documento_ocr,
        factura_duplicada, -- 
        fec_atencion as fec_atencion_duplicado, -- 
        num_lote,
        num_siniestro,
        num_siniestro_factDup, -- b.num_siniestro as num_siniestro_factDup,
        dsc_dup, -- 
        dsc_abono, --
        --dsc_igualdad,
        estado_procesado as estado_procesado_duplicado, --
        monto_sub_factura_ocr,
        monto_factura_ocr,
        monto_duplicado as monto_factura_duplicado, --
        -- FLAGS
        flag_autoseguro_vencido,
        flag_afiliado_NoVigente_rango_cg,
        flag_duplicidad_pago,
        flag_ctdSiteds_trama_autorizaciones_noCoincide,
        flag_afiliadoCPMAtencionesGrupoClinica,
        --
        ruc_proveedor_emisor_ocr,
    -- select * 
    FROM
        `{{project_id}}.siniestro_salud_auna.trama_factura_siteds_reglas`
        WHERE processed_date = PERIODO_INI -- ACTUALIZAR FECHA O PONER EL MAX processed_date
        -- IMPORTANTE: Usa backticks para nombres de tabla en BigQuery
),
CasuisticasDetalladas AS (
    -- CTE 2: Genera una fila por cada casuística de caída detectada en cada factura.
    SELECT
        T.grupo_clinica_texto,
        T.num_factura_documento_ocr,
        T.factura_duplicada,
        T.processed_date,
        T.fecha_emision_siteds_ocr,
        T.fec_atencion_duplicado,
        T.num_lote,
        T.num_siniestro,
        T.num_siniestro_factDup,
        T.dsc_dup,
        T.dsc_abono,
        --T.dsc_igualdad,
        T.estado_procesado_duplicado,
        T.monto_sub_factura_ocr,
        T.monto_factura_ocr,
        T.monto_factura_duplicado,
        T.ruc_proveedor_emisor_ocr,
        --T.nombre_emisor,
        reglas.message AS reglas_mensaje
    FROM
        FacturaDatos AS T,
        UNNEST(ARRAY<STRUCT<condition BOOL, message STRING>>[
            
          /************** LISTA DE REGLAS ************************/

            -- Regla 1: Contratante con autoseguro vencido
            STRUCT(T.flag_autoseguro_vencido = 1, 'Contratante con autoseguro vencido'), -- Modifificar los meses de esta regla

            -- Regla 2: Numero de poliza no vigente en RS entre las fechas de la carta de garantia
            STRUCT(T.flag_afiliado_NoVigente_rango_cg = 1, 'Afiliado no vigente en RS entre las fechas de la carta de garantia'),
            
            -- Regla 3: Afiliado no vigente en RS entre las fechas de la carta de garantia
            STRUCT(T.flag_duplicidad_pago = 1, 'Factura con duplicidad de pago'),

            -- Regla 4: La cantidad de siteds de CPM en la trama no coincide con la cantidad de siteds en la tabla autorizaciones
            STRUCT(T.flag_ctdSiteds_trama_autorizaciones_noCoincide = 1, 'La cantidad de siteds de CPM en la trama no coincide con RS'),

            -- Regla 5: CPM cronico con mas de una atencion en un mismo grupo de clinica
            --STRUCT(T.flag_afiliadoCPMAtencionesGrupoClinica = 1, 'CPM cronico con mas de una atencion en un mismo grupo de clinica')
            

        ]) AS reglas -- 'casuistica' es el alias del elemento unnested (el STRUCT)
    WHERE reglas.condition IS TRUE -- Filtra para incluir solo las casuísticas cuya condición se cumple
),
tabla_final as (

-- Consulta final que combina las casuísticas detectadas y las facturas "OK"
SELECT
    cd.grupo_clinica_texto,
    cd.num_factura_documento_ocr,
    cd.factura_duplicada,
    cd.processed_date,
    cd.fecha_emision_siteds_ocr,
    cd.fec_atencion_duplicado,
    cd.num_lote,
    cd.num_siniestro,
    cd.num_siniestro_factDup,
    cd.dsc_dup,
    cd.dsc_abono,
    --cd.dsc_igualdad,
    cd.estado_procesado_duplicado,
    --cd.nombre_emisor,
    cd.reglas_mensaje,
    cd.monto_sub_factura_ocr,
    cd.monto_factura_ocr,
    cd.monto_factura_duplicado,

    cd.ruc_proveedor_emisor_ocr,
FROM
    CasuisticasDetalladas AS cd

UNION ALL

-- Añade las facturas que NO tienen ninguna casuística detectada.
-- Esto asegura que todas las facturas originales aparezcan al menos una vez.
SELECT
    fd.grupo_clinica_texto,
    fd.num_factura_documento_ocr,
    fd.factura_duplicada,
    fd.processed_date,
    fd.fecha_emision_siteds_ocr,
    fd.fec_atencion_duplicado,
    fd.num_lote,
    fd.num_siniestro,
    fd.num_siniestro_factDup,
    fd.dsc_dup,
    fd.dsc_abono,
    --fd.dsc_igualdad,
    fd.estado_procesado_duplicado,
    --fd.nombre_emisor,
    'Factura OK' AS reglas_mensaje,
    fd.monto_sub_factura_ocr,
    fd.monto_factura_ocr,
    fd.monto_factura_duplicado,

    fd.ruc_proveedor_emisor_ocr,
FROM
    FacturaDatos AS fd
LEFT JOIN
    CasuisticasDetalladas AS cd ON fd.num_factura_documento_ocr = cd.num_factura_documento_ocr
WHERE
    cd.num_factura_documento_ocr IS NULL -- Solo las que no se encontraron en CasuisticasDetalladas
ORDER BY
    num_factura_documento_ocr, reglas_mensaje
),
tabla_fin2 as (
    select DISTINCT a.*,
    case when reglas_mensaje in ('Factura OK','Factura ya registrada en RS') then 'OK' else 'OBSERVADA' end estado
    from tabla_final a
)
select *
from tabla_fin2
;

