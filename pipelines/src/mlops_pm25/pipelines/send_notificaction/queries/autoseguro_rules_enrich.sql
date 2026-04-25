###################################################################################################
############### PASO 01: Creación Tabla de Empresas/Contratantes AutoSeguros  #####################
###################################################################################################

CREATE OR REPLACE TABLE `{{project_id}}.siniestro_salud_auna.auna_listado_empresas_autoseguros` 
AS
SELECT * FROM UNNEST([
  STRUCT(CAST(NULL AS STRING) AS RUC, 'Backus Estrategia S.A.C.' AS razon_social),
  STRUCT('20602207731', 'Backus Servicio de Ventas S.A.C.'),
  STRUCT('20128915711', 'Cervecería San Juan S.A.'),
  STRUCT('20510014279', 'Club Sporting Cristal S.A.'),
  STRUCT('20114915026', 'Compañía Minera Antapaccay S.A.'),
  STRUCT('20161749126', 'Congreso de la Republica'),
  STRUCT('20423924137', 'Fundación Telefónica del Perú'),
  STRUCT('20600299884', 'Gesnext Perú S.A.C.'),
  STRUCT('20501827623', 'Gestión de Servicios Compartidos S.A.C.'),
  STRUCT('20605061096', 'Govertis Advisory Services Perú S.A.C.'),
  STRUCT('20373697720', 'Grupo Repsol del Perú S.A.C.'),
  STRUCT('20607527661', 'Hispasat Perú S.A.C.'),
  STRUCT('20602982174', 'Internet para Todos'),
  STRUCT('20335955065', 'Media Networks Latin America S.A.C.'),
  STRUCT('20501633004', 'Media Networks'),
  STRUCT('20128847881', 'Naviera Oriente S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Refinería La Pampilla S.A.A.'),
  STRUCT('20503840121', 'Repsol Comercial S.A.C.'),
  STRUCT('20258262728', 'Repsol Exploración Perú Sucursal del Perú'),
  STRUCT('20606862556', 'Semod S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Telefónica Cybersecurity Tech Perú S.A.C.'),
  STRUCT('20100070970', 'Telefónica del Perú'),
  STRUCT(CAST(NULL AS STRING), 'Telefónica Factoring Perú'),
  STRUCT('20501827623', 'Telefónica Gestión de Servicios Compartidos Perú S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Telefónica Ingeniería de Seguridad Perú S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Telefónica International Wholesale Services Perú S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Telefónica Learning Services Perú S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Telefónica Móviles'),
  STRUCT(CAST(NULL AS STRING), 'Telefónica Multimedia S.A.C.'),
  STRUCT('20607092851', 'Telefónica On The Spot Soluciones Digitales Perú S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Telefónica Servicios Comerciales'),
  STRUCT(CAST(NULL AS STRING), 'Telefónica Servicios TIWS Sociedad Anónima'),
  STRUCT(CAST(NULL AS STRING), 'Telxius Cable Perú S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Telxius Torres Perú S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Terra Network Perú S.A.'),
  STRUCT(CAST(NULL AS STRING), 'Tgestiona Logística S.A.C.'),
  STRUCT(CAST(NULL AS STRING), 'Tgestiona Servicios Contables y Capital Humano S.A.C.'),
  STRUCT('20606862556', 'Tgestiona Servicios Globales'),
  STRUCT(CAST(NULL AS STRING), 'Transportes 77 S.A.'),
  STRUCT(CAST(NULL AS STRING), 'Unión de Cervecerías Peruanas Backus y Johnston S.A.A.'),
  STRUCT(CAST(NULL AS STRING), 'Wayra Perú Aceleradora de Proyectos S.A.C.')
]);


###################################################################################################
############### YA NO SE EJECUTA LO DE ABAJO #####################
###################################################################################################


-- SELECT DISTINCT nom_sede_proveedor_siniestro FROM `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud`
-- WHERE TRUE
-- --AND nom_sede_proveedor_siniestro
-- AND DATE(DATE_TRUNC(fec_hora_ocurrencia, MONTH)) = '2025-01-01'


