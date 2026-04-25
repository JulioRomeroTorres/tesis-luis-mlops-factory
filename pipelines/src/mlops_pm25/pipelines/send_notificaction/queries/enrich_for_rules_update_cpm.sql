###################################################################################################
################### PASO 1: Actualizar tabla historica de reglas enrich  ##########################
###################################################################################################

UPDATE `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas` AS A
SET 
  A.flag_consultaAmboMed_liqui_ocr = B.flag_consultaAmboMed_liqui_ocr,
  A.flag_montoTotal_NoCoincide_Fact_Liqui_ocr = B.flag_montoTotal_NoCoincide_Fact_Liqui_ocr,
  A.flag_fecIngreso_liqui_ocr_menor_fecAutoSiteds_ocr = B.flag_fecIngreso_liqui_ocr_menor_fecAutoSiteds_ocr,
  A.flag_fecIngreso_liqui_ocr_mayor15d_fecAutoSiteds_ocr = B.flag_fecIngreso_liqui_ocr_mayor15d_fecAutoSiteds_ocr,
  A.flag_dniPaciente_Siteds_ocr_NoCoincide_Liqui_ocr = B.flag_dniPaciente_Siteds_ocr_NoCoincide_Liqui_ocr,
  A.flag_codAutorizacion_Siteds_ocr_NoCoincide_Liqui_ocr = B.flag_codAutorizacion_Siteds_ocr_NoCoincide_Liqui_ocr,
  A.flag_deduciblePaciente_Siteds_ocr_NoCoincide_Liqui_ocr = B.flag_deduciblePaciente_Siteds_ocr_NoCoincide_Liqui_ocr
FROM `{{project_id}}.siniestro_salud_auna.base_enrich_for_reglas_cpm_grouped` AS B
WHERE 
  A.num_factura_liqui_ocr = B.num_factura_liqui_ocr AND
  A.ruc_proveedor_emisor_ocr = B.ruc_liqui_ocr;