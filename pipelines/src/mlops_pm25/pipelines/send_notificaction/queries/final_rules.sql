-- #############################################################################
-- #                       REGLAS INTELIGENTES FACTURACION
-- #############################################################################



####################################
######### Fuentes Usadas ###########
####################################
-- 1. `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas` -- Esto fue creado en Enrich_for_rules
######################################

DECLARE PERIODO_INI STRING;

SET PERIODO_INI = (
    SELECT IF(
        '{{process_period}}' = '',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('America/Lima'), INTERVAL 1 DAY)),
        '{{process_period}}'
    )
);


DELETE FROM `{{project_id}}.siniestro_salud_auna.base_reglas_finales`
WHERE processed_date = PERIODO_INI;

-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.base_reglas_finales`
--CREATE OR REPLACE TABLE
INSERT INTO
  `{{project_id}}.siniestro_salud_auna.base_reglas_finales`
  
WITH FacturaDatos AS (
    -- CTE 1: Define tu tabla de origen y selecciona las columnas necesarias.
    -- Reemplaza 'your_project.your_dataset.your_table' con la ruta completa a tu tabla en BigQuery.
    SELECT
        processed_date,
        fecha_emision_siteds_ocr,
        num_factura_documento_ocr,
        factura_duplicada,
        fec_atencion as fec_atencion_duplicado,
        num_siniestro,
        num_siniestro_factDup,
        dsc_dup,
        dsc_abono,
        --dsc_igualdad,
        estado_procesado as estado_procesado_duplicado,
        monto_sub_factura_ocr,
        monto_factura_ocr,
        monto_factura_trama,
        monto_duplicado as monto_factura_duplicado,
        importe_total_ocr_nc,
        montoSub_nc_trama,
        --nombre_emisor,
--        ratio_coincidencia,
        flag_sinSiteds,
        flag_anomaly,
        NULL AS flag_factura_conocida,
        flag_rucRimac_Fact_NoCoincide_producto_Siteds_ocr,
        flag_producto_rs_coincide,
        flag_dni_rs_coincide,
        flag_dni_perdetalle_coincide,
        flag_duplicidad_pago,
        flag_abono, -- esto no va como regla
        flag_conNC_trama_sinNC_sustento,
        flag_sinNC_trama_conNC_sustento,
        flag_conNC_trama_conNC_sustento,
        flag_fechaEmisionNC_noCoincide_sustento_trama,
        flag_montoTotal_NoCoincide_Fact_Preli_ocr,
        flag_fecIngreso_mayor_fecAlta_preliqui_ocr,
        flag_fecAutoSiteds_ocr_NoCoincide_fecIngreso_preliqui_ocr,
        flag_montoTotal_NoCoincide_Fact_Liqui_ocr,
        flag_fecIngreso_mayor_fecAlta_liqui_ocr,
        flag_mecanismoPago_Liquidacion_ocr_NoCoincide_RS,
        --flag_fecAutoSiteds_ocr_NoCoincide_fecIngreso_liqui_ocr,
        flag_fecIngreso_liqui_ocr_menor_fecAutoSiteds_ocr,
        flag_fecIngreso_liqui_ocr_mayor15d_fecAutoSiteds_ocr,
        flag_dniPaciente_Siteds_ocr_NoCoincide_Liqui_ocr,
        flag_dniPaciente_Siteds_ocr_NoCoincide_RS,
        flag_pctCopagoPaciente_Siteds_ocr_NoCoincide_pctCoaseguroPaciente_Liqui_ocr,
        flag_deduciblePaciente_Siteds_ocr_NoCoincide_Liqui_ocr,
        flag_cg_anulada,
        flag_conCG_trama_sinCG_sustento,
        flag_sinCG_trama_conCG_sustento,
        flag_conCG_trama_conCG_sustento,
        flag_monto_excede_cg,
        flag_mecanismoPago_Liquidacion_ocr_NoCoincide_trama,
        flag_subtotal_Liqui_ocr_NoCoincide_Fact_trama,
        flag_codAutorizacion_Siteds_ocr_NoCoincide_Liqui_ocr,
        flag_razonSocial_fact_ocr_NoCoincide_cg_ocr,
        flag_ruc_fact_ocr_NoCoincide_cg_ocr,
        flag_cg_dobleSiniestro,
        flag_subtotalNC_noCoincide_sustento_trama,
        flag_totalNC_noCoincide_RS,
        flag_fechasHosp_NoCoincide_diasHosp_epi_ocr,
        flag_fecIngreso_epi_ocr_menor_fecSiteds_ocr,
        flag_dxSiteds_ocr_NoCoincide_trama,
        flag_coberturaSiteds_ocr_NoCoincide_trama,
        flag_sinReceta_ambulatorio,
        flag_dxEntrada_CG_NoCoincide_dxEntrada_epi,
        flag_autoseguro_vencido,
        flag_fecFact_mayor_fecSiteds_ocr,
        flag_MtoTotal_ocr_NoCoincide_Trama,
        flag_fecAtencion_NoCoincide_rango_cg,
        flag_afiliadoNoVigenteAtencion,
        flag_afiliado_NoVigente_rango_cg,
        --flag_cg_dobleFactura,
        monto_final_cg,
        -- des_est_cuenta_por_pagar,
        coincide_plan,
--        deteccion_outlier,
        diferencia_dias_fecha_factura_prestacion,
        modelo, -- select distinct agrupacion_cobertura_negocio
        umbral,
        agrupacion_cobertura_negocio, 
        ruc_proveedor_emisor_ocr,
        nom_comercial_proveedor_siniestro,
        nom_sede_proveedor_siniestro,
    -- select * 
    FROM
        `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas` 
        where processed_date = PERIODO_INI
        --WHERE num_factura_documento_ocr = 'F10100010480'
        -- IMPORTANTE: Usa backticks para nombres de tabla en BigQuery
),
CasuisticasDetalladas AS (
    -- CTE 2: Genera una fila por cada casuística de caída detectada en cada factura.
    SELECT
        T.num_factura_documento_ocr,
        T.factura_duplicada,
        T.processed_date,
        T.fecha_emision_siteds_ocr,
        T.fec_atencion_duplicado,
        T.num_siniestro,
        T.num_siniestro_factDup,
        T.dsc_dup,
        T.dsc_abono,
        --T.dsc_igualdad,
        T.estado_procesado_duplicado,
        T.monto_sub_factura_ocr,
        T.monto_factura_ocr,
        T.monto_factura_trama,
        T.monto_factura_duplicado,
        T.importe_total_ocr_nc,
        T.montoSub_nc_trama,
        T.monto_final_cg,
        T.modelo,
        T.umbral,
        T.agrupacion_cobertura_negocio, 
        T.ruc_proveedor_emisor_ocr,
        --T.nombre_emisor,
        reglas.message AS reglas_mensaje
    FROM
        FacturaDatos AS T,
        UNNEST(ARRAY<STRUCT<condition BOOL, message STRING>>[
            
          /************** LISTA DE REGLAS ************************/
            
             -- Regla 1: Detección de monto atípico
            --STRUCT(T.flag_anomaly = 1, 'Detección de Monto Atípico'),
            STRUCT(T.flag_anomaly = 1,
                CONCAT(
                    'Deteccion de Monto Atipico: El monto facturado es mucho mayor al monto historico promedio (S/. ',
                    COALESCE(T.umbral, 0),
                    ') registrado para la cobertura (',
                    COALESCE(T.agrupacion_cobertura_negocio, '-'),
                    ') en clínica (',
                    COALESCE(T.nom_sede_proveedor_siniestro, '-'),
                    ')'
                )
            ),

            
            -- Regla 2: Factura ya registrada en RS
#            STRUCT(T.flag_factura_conocida = 1, 'Factura ya registrada en RS'),
            
            -- Regla 2: RUC Rimac en Factura no coincide con producto usado en SITEDS (Rimac Seguros y Rimac EPS)
            --(RA025)
            STRUCT(T.flag_rucRimac_Fact_NoCoincide_producto_Siteds_ocr = 1, 'RUC Rimac en Factura no coincide con producto usado en SITEDS'), 
            
            -- Regla 3: Plan mal emitido
#            STRUCT(T.coincide_plan = 0, 'Factura no coincide con Plan de cliente'),

            -- Regla 4: Facturada generada antes que SITED
            --STRUCT(T.diferencia_dias_fecha_factura_prestacion IS NOT NULL AND T.diferencia_dias_fecha_factura_prestacion < 0, 'Facturación emitida antes que SITED'),--, -- Umbral de 30 días
            
            -- Regla 5: Ratio de coincidencia (OCR vs TRAMA)
--            STRUCT(T.ratio_coincidencia < 1, 'Diferencia con trama')--,

            -- Regla 6: DNI no coincide con RS
            -- (RA012)
            STRUCT(T.flag_dni_rs_coincide = 0 and T.flag_dni_perdetalle_coincide != 1, 'Doc Identidad Paciente no coincide con RS'),

            -- Regla 7: Producto no coincide con RS
#            STRUCT(T.flag_producto_rs_coincide = 0, 'Producto no coincide con RS'),

            -- Regla 8: Factura no tiene SITEDS
            STRUCT(T.flag_sinSiteds = 1, 'Factura no tiene hoja SITEDS'),
            
            -- Regla 9: Duplicidad de Pago
            STRUCT(T.flag_duplicidad_pago = 1, 'Factura con duplicidad de pago'),

            -- Regla 10: Factura con abono
            -- STRUCT(T.flag_abono = 1, 'Factura con abono')

            -- Añadir flag de que no tenga trama
            
            -- Regla 11: Factura con NC en la trama pero no en el sustento
            -- (RA008)
            STRUCT(T.flag_conNC_trama_sinNC_sustento = 1, 'Con Nota Credito en la trama pero no en el sustento'),

            -- Regla 12: Factura sin NC en la trama pero si en el sustento
            STRUCT(T.flag_sinNC_trama_conNC_sustento = 1, 'Sin Nota Credito en la trama pero si en el sustento'),

            -- Regla 13: Fecha de NC y el de la trama no coinciden (EXTRA)
            STRUCT(T.flag_fechaEmisionNC_noCoincide_sustento_trama = 1, 'La fecha de emision de NC y Trama no coinciden'),

            -- Regla 14: Monto total de NC en sustento y trama no coinciden (el monto de la trama es subtotal, es lo unico que hay)
            -- Pedir a July que agrege subtotal del OCR de NC
            
            -- Regla 15: Monto de factura no coincide con preliquidacion
            --STRUCT(T.flag_montoTotal_NoCoincide_Fact_Preli_ocr = 1, 'El monto total de la Factura no coincide con el de Preliquidacion'),

            -- Regla 16: Monto de preliquidación no coincide con liquidación (revisar si estos montos deberían ser iguales) (EXTRA)


            -- Regla 14: Fecha de ingreso posterior a la fecha alta de Preliquidacion
            STRUCT(T.flag_fecIngreso_mayor_fecAlta_preliqui_ocr = 1, 'La fecha de ingreso es posterior a la fecha de alta en la hoja Preliquidacion'),

            -- Regla 15: Fecha de autorizacion de SITEDS no coincide con Fecha de Ingreso Preliquidacion
            --STRUCT(T.flag_fecAutoSiteds_ocr_NoCoincide_fecIngreso_preliqui_ocr = 1, 'La fecha de autorizacion de Siteds y la fecha de ingreso en la Preliquidacion no coinciden'),
            
            -- Regla 16: Monto de factura no coincide con Liquidacion
            STRUCT(T.flag_montoTotal_NoCoincide_Fact_Liqui_ocr = 1, 'El monto total de la Factura no coincide con el de Liquidacion'),

            -- Regla 17: Fecha de ingreso posterior a la fecha alta de Liquidacion
            STRUCT(T.flag_fecIngreso_mayor_fecAlta_liqui_ocr = 1, 'La fecha de ingreso es posterior a la fecha de alta en la hoja Liquidacion'),

            -- Regla 18: Fecha de autorizacion de SITEDS no coincide con Fecha de Ingreso Liquidacion
            -- (RA035)
            --STRUCT(T.flag_fecAutoSiteds_ocr_NoCoincide_fecIngreso_liqui_ocr = 1, 'La fecha de autorizacion de Siteds y la fecha de ingreso en la Liquidacion no coinciden'),

            -- Regla 18: Fecha de Ingreso Liquidacion antes que la fecha de autorización en Siteds
            -- (RA035)
            -- STRUCT(T.flag_fecIngreso_liqui_ocr_menor_fecAutoSiteds_ocr = 1, 'La fecha de autorizacion de Siteds posterior a la fecha de ingreso en la Liquidacion'),
            STRUCT(T.flag_fecIngreso_liqui_ocr_menor_fecAutoSiteds_ocr = 1, 'La fecha de autorizacion de Siteds posterior a la fecha de ingreso en mas de 7 dias'),

            -- Regla 18: La fecha de ingreso en la Liquidacion posterior a la fecha de autorizacion de Siteds en mas de 14 dias
            -- (RA035)
            STRUCT(T.flag_fecIngreso_liqui_ocr_mayor15d_fecAutoSiteds_ocr = 1, 'La fecha de ingreso en la Liquidacion posterior a la fecha de autorizacion de Siteds en mas de 14 dias'),

            -- Regla 18: Documento de identidad de SITEDS no coincide con el de Liquidacion
            -- (RA028)
            -- STRUCT(T.flag_dniPaciente_Siteds_ocr_NoCoincide_Liqui_ocr = 1, 'El documento de identidad del paciente en Siteds no coincide con el de Liquidacion'),

            -- Regla 18: Documento de identidad de SITEDS no coincide con el de RS
            -- (RA028)
            -- STRUCT(T.flag_dniPaciente_Siteds_ocr_NoCoincide_RS = 1, 'El documento de identidad del paciente en Siteds no coincide con el RS'),

            -- Regla 18: Cod Autorizacion de la hoja SITEDS no coincide con cod autorizacion de la hoja LIQUIDACION
            -- (RA041)
            STRUCT(T.flag_codAutorizacion_Siteds_ocr_NoCoincide_Liqui_ocr = 1, 'El codigo de autorizacion de la hoja siteds no coincide con el de Liquidacion'),    
            
            -- Regla 18: El % cubierto del copago variable SITEDS no coincide con coaseguro del paciente en Liquidacion
            STRUCT(T.flag_pctCopagoPaciente_Siteds_ocr_NoCoincide_pctCoaseguroPaciente_Liqui_ocr = 1, 'El % cubierto del copago variable del SITEDS no coincide con el % de coaseguro del paciente en Liquidacion'),

            -- Regla 18: Deducible del paciente en SITEDS no coincide con el de Liquidacion
            --(RA048)
            STRUCT(T.flag_deduciblePaciente_Siteds_ocr_NoCoincide_Liqui_ocr = 1, 'El deducible del Paciente en SITEDS no coincide con el de Liquidacion'),
            
            -- Regla 18: Mecanismo de pago de Liquidacion no coincide con RS
            STRUCT(T.flag_mecanismoPago_Liquidacion_ocr_NoCoincide_RS = 1, 'El mecanismo de pago de Liquidacion no coincide con RS'),

            -- Regla 19: Factura con CG en trama pero no sustento
            --(RA004)
            STRUCT(T.flag_conCG_trama_sinCG_sustento = 1, 'Con Carta Garantia en la trama pero no en el sustento'),

            -- Regla 20: Factura sin CG en trama pero si sustento
            STRUCT(T.flag_sinCG_trama_conCG_sustento = 1, 'Sin Carta Garantia en la trama pero si en el sustento'),

            -- Regla 21: Factura con monto mayor al de CG del sustento
            --()(RA004)
            STRUCT(T.flag_monto_excede_cg = 1, 'El monto de facturacion excede el de Carta Garantia del sustento'),

            -- Regla 24: CG anulada en Rimac Salud
            STRUCT(T.flag_cg_anulada = 1, 'La carta de garantia se encuentra Anulada en RS'),

            -- Regla 25: Mecanismo de pago Liquidacion no coincide con Mecanismo de Trama
            STRUCT(T.flag_mecanismoPago_Liquidacion_ocr_NoCoincide_trama = 1, 'El mecanismo de pago de liquidación del sustento no coincide con el de la trama'),

            -- Regla 26: Monto subtotal de liquidacion no coincide con el de la trama
            STRUCT(T.flag_subtotal_Liqui_ocr_NoCoincide_Fact_trama = 1, 'El monto subtotal de liquidación del sustento no coincide con el de la trama'),

            -- Regla 27: Razon social de la Factura no coincide con la de la carta de garantia
            -- (RA004)
            STRUCT(T.flag_razonSocial_fact_ocr_NoCoincide_cg_ocr = 1, 'La razon social de Factura no concide con la de la carta de garantia'),
            
            -- Regla 28: RUC de la Factura no coincide con la de la carta de garantia
            -- (RA004)
            STRUCT(T.flag_ruc_fact_ocr_NoCoincide_cg_ocr = 1, 'El RUC de Factura no concide con el de la carta de garantia'), -- redactar bien

            -- Regla 28: La CG de la trama está asociada a más de un siniestro
            -- (RA040)
            STRUCT(T.flag_cg_dobleSiniestro = 1, 'La Carta de Garantia de la trama esta asociada a mas de un siniestro'),

            -- Regla 28: La CG de la trama está asociada a más de una factura
            -- (RA040)
            --STRUCT(T.flag_cg_dobleFactura = 1, 'La CG de la trama esta asociada a mas de una factura')

            -- Regla 28: El monto subtotal de NC y Trama no coincide
            -- (RA053)
            -- STRUCT(T.flag_subtotalNC_noCoincide_sustento_trama = 1, 'El monto subtotal de NC y Trama no coincide'),

            -- Regla 28: El monto total de NC y RS no coincide
            -- (RA053)
            STRUCT(T.flag_totalNC_noCoincide_RS = 1, 'El monto total de NC y RS no coincide'),

            -- Regla 28: Fecha de Egreso vs Ingreso menor a dias de estadia en Epicrisis
            -- (RA020)
            STRUCT(T.flag_fechasHosp_NoCoincide_diasHosp_epi_ocr = 1, 'Fechas de hospitalizacion y los dias de estadia no coincide en el sustento de Epicrisis'),

            -- Regla 28: La fecha de Ingreso de Epicrisis menor a fecha de autorización de siteds 
            -- (RA020)
            STRUCT(T.flag_fecIngreso_epi_ocr_menor_fecSiteds_ocr = 1, 'Fecha de ingreso en epicrisis menor a la fecha de siteds en el sustento'),
            
            -- Regla 28: El diagnostico de SITEDS no coincide con el de la Trama
            -- (RA034)
            --STRUCT(T.flag_dxSiteds_ocr_NoCoincide_trama = 1, 'El diagnostico de siteds en el sustento no coincide con el de la trama'),

            -- Regla 28: La cobertura de SITEDS no coincide con el de la Trama
            -- (RA034) (RA038)
            --STRUCT(T.flag_coberturaSiteds_ocr_NoCoincide_trama = 1, 'La cobertura de Siteds no coincide con el de la trama'),

            -- Regla 28: Ambulatorio con medicamento sin receta medica en el sustento
            -- (RA032)
            STRUCT(T.flag_sinReceta_ambulatorio = 1, 'Ambulatorio con medicamento sin receta medica en el sustento'),

            -- Regla 28: Hospitalarios con DX Entrada CG no coincide con el de Epicrisis
            -- (RA016)
            STRUCT(T.flag_dxEntrada_CG_NoCoincide_dxEntrada_epi = 1, 'Hospitalarios con diagnostico de entrada CG no coincide con el de Epicrisis en el sustento'),

            -- Regla 28: Hospitalarios con DX Entrada CG no coincide con el de Epicrisis
            -- (RA052)
            STRUCT(T.flag_autoseguro_vencido = 1, 'Contratante con autoseguro vencido tras los 12 meses'),

            -- Regla 28: Fecha de autorizacion siteds posterior a fecha de facturacion en el sustento
            -- (RA052)
            STRUCT(T.flag_fecFact_mayor_fecSiteds_ocr = 1, 'Fecha de autorizacion siteds posterior a fecha de facturacion en el sustento'),

            -- Regla 28: Monto Total de Factura vs Monto Total de Trama TEDEF deben ser exactamente iguales.
            -- (SIN ID)
            STRUCT(T.flag_MtoTotal_ocr_NoCoincide_Trama = 1, 'El monto total de la factura no coincide con el de la trama'),

            -- Regla 28: La fecha de atencion no esta dentro del rango de fechas de carta de garantia
            -- (SIN ID)
            STRUCT(T.flag_fecAtencion_NoCoincide_rango_cg = 1, 'La fecha de atencion no esta dentro del rango de fechas de carta de garantia en el sustento'),
            -- Regla 28: Monto Total de Factura vs Monto Total de Trama TEDEF deben ser exactamente iguales.
            -- (SIN ID)
            STRUCT(T.flag_afiliadoNoVigenteAtencion = 1, 'Afiliado no vigente en RS durante la fecha de atencion en siteds'),

            -- Regla 28: Monto Total de Factura vs Monto Total de Trama TEDEF deben ser exactamente iguales.
            -- (SIN ID)
            STRUCT(T.flag_afiliado_NoVigente_rango_cg = 1, 'Afiliado no vigente en RS entre las fechas de la carta de garantia')    
            

        ]) AS reglas -- 'casuistica' es el alias del elemento unnested (el STRUCT)
    WHERE reglas.condition IS TRUE -- Filtra para incluir solo las casuísticas cuya condición se cumple
),
tabla_final as (

-- Consulta final que combina las casuísticas detectadas y las facturas "OK"
SELECT
    cd.num_factura_documento_ocr,
    cd.factura_duplicada,
    cd.processed_date,
    cd.fecha_emision_siteds_ocr,
    cd.fec_atencion_duplicado,
    cd.num_siniestro,
    cd.num_siniestro_factDup,
    cd.dsc_dup,
    cd.dsc_abono,
    --cd.dsc_igualdad,
    cd.estado_procesado_duplicado,
    --cd.nombre_emisor,
    cd.reglas_mensaje,
    cd.monto_sub_factura_ocr,
    cd.monto_factura_trama,
    cd.monto_factura_ocr,
    cd.monto_factura_duplicado,
    cd.importe_total_ocr_nc,
    cd.montoSub_nc_trama,
    cd.monto_final_cg,
    cd.modelo,
    cd.umbral,
    cd.agrupacion_cobertura_negocio, 
    cd.ruc_proveedor_emisor_ocr,
FROM
    CasuisticasDetalladas AS cd

UNION ALL

-- Añade las facturas que NO tienen ninguna casuística detectada.
-- Esto asegura que todas las facturas originales aparezcan al menos una vez.
SELECT
    fd.num_factura_documento_ocr,
    fd.factura_duplicada,
    fd.processed_date,
    fd.fecha_emision_siteds_ocr,
    fd.fec_atencion_duplicado,
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
    fd.monto_factura_trama,
    fd.monto_factura_duplicado,
    fd.importe_total_ocr_nc,
    fd.montoSub_nc_trama,
    fd.monto_final_cg,
    fd.modelo,
    fd.umbral,
    fd.agrupacion_cobertura_negocio, 
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
    select a.*,
    case when reglas_mensaje in ('Factura OK','Factura ya registrada en RS') then 'OK' else 'OBSERVADA' end estado
    from tabla_final a
)
select *
from tabla_fin2
;

-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.base_reglas_finales` WHERE num_factura_documento_ocr= 'F77500079671'

-- SELECT DISTINCT reglas_mensaje FROM `{{project_id}}.siniestro_salud_auna.base_reglas_finales`
-- SELECT * FROM `{{project_id}}.siniestro_salud_auna.base_reglas_finales` WHERE reglas_mensaje LIKE 'Deteccion de Monto Atipico: monto excede al promedio historico por sede y cobertura%'


