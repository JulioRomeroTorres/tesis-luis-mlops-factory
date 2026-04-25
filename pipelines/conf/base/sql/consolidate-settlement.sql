INSERT INTO `rs-nprd-dlk-ia-dev-aif-d3d9.genai_documents.auna_consolidated_settlemet`
(
  documento_id,
  page_path,
  cabecera,
  calculo_cpm,
  created_at,
  type
) (
    select 
    documento_id,
    page_path,
    (
    SELECT AS STRUCT s.cabecera.* REPLACE (
      SAFE_CAST(cabecera.deducible AS STRING) AS deducible,
      SAFE_CAST(cabecera.coaseguro AS STRING) AS coaseguro
    ),
    CAST(NULL AS STRING) AS num_documento,
    CAST(NULL AS STRING) as num_factura,
  ) AS cabecera,
    calculo_cpm,
    created_at,
    'SUMMARY' as type 
    from `rs-nprd-dlk-ia-dev-aif-d3d9.genai_documents.auna_settlement_summary_mvp` as s
    where created_at >= COALESCE((SELECT MAX(created_at) from `rs-nprd-dlk-ia-dev-aif-d3d9.genai_documents.auna_consolidated_settlemet` where type = 'SUMMARY'), TIMESTAMP('1900-01-01'))
);

INSERT INTO `rs-nprd-dlk-ia-dev-aif-d3d9.genai_documents.auna_consolidated_settlemet`
(
  documento_id,
  page_path,
  cabecera,
  subgrupos,
  gastos_afectos,
  gastos_inafectos,
  created_at,
  type
) (
    select 
    documento_id,
    page_path,
    (
      SELECT AS STRUCT 
      
      s.cabecera.titulo,
      s.cabecera.sede,
      s.cabecera.garante,
      s.cabecera.empresa,
      SAFE_CAST(s.cabecera.deducible AS STRING) AS deducible,
      SAFE_CAST(s.cabecera.coaseguro AS STRING) AS coaseguro,
      s.cabecera.num_autoriz,
      s.cabecera.paciente,
      s.cabecera.titular,
      s.cabecera.tratante,
      s.cabecera.hist_clin,
      CAST(NULL AS STRING) as num_cama,  
      s.cabecera.fec_ingreso,
      s.cabecera.fec_alta,
      s.cabecera.num_encuentro,
      s.cabecera.beneficio,
      s.cabecera.mecanismo,
      s.cabecera.monto_cpm,
      s.cabecera.tipo_moneda,
      CAST(NULL AS STRING) AS num_documento, 
      CAST(NULL AS STRING) AS num_factura   
    ) AS cabecera,
    ARRAY(
    SELECT AS STRUCT
      sg.categoria,
      ARRAY(
        SELECT AS STRUCT
          i.codigo,
          i.descripcion,
          i.fec_registro,
          i.cantidad,
          i.precio_unitario,
          i.importe,
          i.igv,
          i.coaseguro,
          i.paciente,
          i.importe_garante,
          CAST(NULL AS NUMERIC) AS total_inafectos,
          CAST(NULL AS NUMERIC) AS total_afectos
        FROM UNNEST(sg.items) AS i
      ) AS items,
      STRUCT(
        sg.subtotal.precio_unitario AS precio_unitario,
        sg.subtotal.importe AS importe,
        sg.subtotal.paciente AS paciente,
        sg.subtotal.importe_garante as importe_garante,
        CAST(NULL AS NUMERIC) AS total_inafectos,
        CAST(NULL AS NUMERIC) AS total_afectos
      ) AS subtotal
    FROM UNNEST(s.subgrupos) AS sg
  ) AS subgrupos,
    (
      SELECT AS STRUCT 
      
      s.gastos_afectos.total_garante as total_garante,
      SAFE_CAST(null AS NUMERIC) AS subtotal_1,
      s.gastos_afectos.deducible_igv as deducible_igv,
      SAFE_CAST(null AS NUMERIC) AS subtotal_2,
      s.gastos_afectos.coaseguro_igv as coaseguro_igv,
      SAFE_CAST(null AS NUMERIC) AS subtotal_3,
      SAFE_CAST(null AS NUMERIC) AS igv,
      SAFE_CAST(null AS NUMERIC) AS total_facturar   
    ) AS gastos_afectos,
    (
      SELECT AS STRUCT 
      
      s.gastos_inafectos.total_garante as total_garante,
      SAFE_CAST(null AS NUMERIC) AS subtotal_1,
      s.gastos_inafectos.coaseguro as coaseguro 
    ) AS gastos_inafectos,
    created_at,
    'T1' as type 
    from `rs-nprd-dlk-ia-dev-aif-d3d9.genai_documents.auna_settlement_type_1_mvp` as s
    where created_at >= COALESCE((SELECT MAX(created_at) from `rs-nprd-dlk-ia-dev-aif-d3d9.genai_documents.auna_consolidated_settlemet` where type = 'T1'), TIMESTAMP('1900-01-01'))
);

INSERT INTO `rs-nprd-dlk-ia-dev-aif-d3d9.genai_documents.auna_consolidated_settlemet`
(
  documento_id,
  page_path,
  cabecera,
  subgrupos,
  gastos_afectos,
  gastos_inafectos,
  created_at,
  type
) (
    select 
    documento_id,
    page_path,
    (
      SELECT AS STRUCT 
      
      s.cabecera.titulo,
      SAFE_CAST(null AS STRING) AS sede,
      s.cabecera.garante,
      s.cabecera.empresa,
      s.cabecera.deducible AS deducible,
      s.cabecera.coaseguro,
      s.cabecera.num_autoriz,
      s.cabecera.paciente,
      s.cabecera.titular,
      s.cabecera.tratante,
      s.cabecera.hist_clin,
      SAFE_CAST(null AS STRING) AS num_cama, 
      s.cabecera.fec_ingreso,
      s.cabecera.fec_alta,
      s.cabecera.num_encuentro,
      s.cabecera.beneficio,
      s.cabecera.mecanismo,
      SAFE_CAST(null AS NUMERIC) AS monto_cpm,
      SAFE_CAST(null AS STRING) AS tipo_moneda, 
      s.cabecera.num_documento, 
      s.cabecera.num_factura   
    ) AS cabecera,
    ARRAY(
    SELECT AS STRUCT
      sg.categoria,
      ARRAY(
        SELECT AS STRUCT
          i.codigo,
          i.descripcion,
          i.fec_registro,
          i.cantidad,
          i.precio_unitario,
          i.importe,
          i.igv,
          i.coaseguro,
          i.paciente,
          CAST(NULL AS NUMERIC) AS importe_garante,
          i.total_inafectos AS total_inafectos,
          i.total_afectos AS total_afectos
        FROM UNNEST(sg.items) AS i
      ) AS items,
      STRUCT(
        sg.subtotal.precio_unitario AS precio_unitario,
        sg.subtotal.importe AS importe,
        sg.subtotal.paciente AS paciente,
        CAST(NULL AS NUMERIC) as importe_garante,
        sg.subtotal.total_inafectos,
        sg.subtotal.total_afectos
      ) AS subtotal
    FROM UNNEST(s.subgrupos) AS sg
  ) AS subgrupos,
    (
      SELECT AS STRUCT 
      
      SAFE_CAST(null AS NUMERIC) as total_garante,
      s.gastos_afectos.subtotal_1,
      s.gastos_afectos.deducible_igv as deducible_igv,
      s.gastos_afectos.subtotal_2,
      s.gastos_afectos.coaseguro_igv as coaseguro_igv,
      s.gastos_afectos.subtotal_3,
      s.gastos_afectos.igv,
      s.gastos_afectos.total_facturar   
    ) AS gastos_afectos,
    (
      SELECT AS STRUCT 
      
      s.gastos_inafectos.total_garante as total_garante,
      s.gastos_inafectos.subtotal_1,
      s.gastos_inafectos.coaseguro as coaseguro 
    ) AS gastos_inafectos,
    created_at,
    'T2' as type 
    from `rs-nprd-dlk-ia-dev-aif-d3d9.genai_documents.auna_settlement_type_2_mvp` as s
    where created_at >= COALESCE((SELECT MAX(created_at) from `rs-nprd-dlk-ia-dev-aif-d3d9.genai_documents.auna_consolidated_settlemet` where type = 'T2'), TIMESTAMP('1900-01-01'))
);