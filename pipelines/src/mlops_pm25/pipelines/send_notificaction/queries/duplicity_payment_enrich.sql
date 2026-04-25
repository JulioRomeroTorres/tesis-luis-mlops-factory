



 
-------base siniestros salud

create or replace table `{{project_id}}.siniestro_salud_auna.tmp_rpt_anl_procesadas`
as 
(

select distinct
--coalesce(concat(cod_serie_comprobante_siniestro,lpad(num_comprobante_siniestro,8,'0')),doc.NRO_DOCUMENTO_PRD) factura,
concat(cod_serie_comprobante_siniestro,lpad(num_comprobante_siniestro,8,'0')) factura,
replace(siniestro.id_autorizacion,'RS-','') id_autorizacion,
auts.fec_autorizacion,
--case when nom_compania='RIMAC EPS' then '1'  when nom_compania='RIMAC SEGUROS' then '2'  end cia,
substr(id_siniestro_origen,1,1) cia,
mto_auditado_sol imp_documento,
num_documento_proveedor_siniestro,
num_poliza,
case when REGEXP_CONTAINS(siniestro.num_documento_afiliado, r'^[0-9]+$') THEN LPAD(CAST(CAST(siniestro.num_documento_afiliado AS INT64) AS STRING), 8,'0') else siniestro.num_documento_afiliado end num_documento_afiliado,
--coalesce(fec_comprobante_siniestro,cast(doc.FEC_DOCUMENTO as date)) fec_comprobante_siniestro,
fec_comprobante_siniestro,
ats.num_diagnostico_origen,
ats.id_cobertura_origen,
--datos extras
replace(siniestro.id_persona_afiliado,'AX-','') id_persona_afiliado,
num_siniestro,
--coalesce(siniestro.num_obligacion,concat('AX-',doc.NUM_OBLIGACION)) num_obligacion,
id_estado_siniestro_origen,
des_estado_siniestro_origen,
ob.cod_est_pago est_cuenta_por_pagar,
replace(id_contratante,'AX-','') id_contratante,
fec_ult_liquidacion,
fec_hora_ocurrencia,
fec_notificacion,
replace(id_persona_proveedor_siniestro,'AX-','') id_persona_proveedor_siniestro,
cod_sede_proveedor_siniestro,
replace(siniestro.id_producto,'AX-','') id_producto,
doc.cod_producto,
cod_mecanismo_pago,
############### NUEVO ###############
cod_tipo_contrato,
#####################################
concat(ctg.anio_carta_garantia,'-',ctg.num_carta_garantia) carta_garantia,
case when id_estado_siniestro_origen='5' then max(dev.num_correlativo_devolucion)over(partition by num_siniestro,dev.des_cod_motivo_devolucion order by dev.num_correlativo_devolucion desc,dev.fec_devolucion_siniestro desc) end item_dev,
case when id_estado_siniestro_origen='5' then max(dev.des_cod_grupo_devolucion)over(partition by num_siniestro,dev.des_cod_motivo_devolucion order by dev.num_correlativo_devolucion desc,dev.fec_devolucion_siniestro desc) end auditoria,
case when id_estado_siniestro_origen='5' then max(dev.des_cod_motivo_devolucion)over(partition by num_siniestro,dev.des_cod_motivo_devolucion order by dev.num_correlativo_devolucion desc,dev.fec_devolucion_siniestro desc) end motivo,
case when id_estado_siniestro_origen='5' then max(dev.fec_devolucion_siniestro)over(partition by num_siniestro,dev.des_cod_motivo_devolucion order by dev.num_correlativo_devolucion desc,dev.fec_devolucion_siniestro desc) end fec_devolucion,
case 
  when id_estado_siniestro_origen='5' and max(dev.cod_motivo_devolucion)over(partition by num_siniestro,dev.des_cod_motivo_devolucion order by dev.num_correlativo_devolucion desc,dev.fec_devolucion_siniestro desc) in (382,376) then 'Rechazo_Definitivo' 
  when id_estado_siniestro_origen='5' and max(dev.cod_motivo_devolucion)over(partition by num_siniestro,dev.des_cod_motivo_devolucion order by dev.num_correlativo_devolucion desc,dev.fec_devolucion_siniestro desc) not in (382,376) then 'Rechazo_No_Definitivo' 
  end ind_devoluc
from   `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud` siniestro
left join unnest(atencion_salud) ats
left join unnest(grupo_devolucion) dev
left join unnest(diagnostico_principal) dxp
left join  `{{anl_project_id}}.anl_siniestro.autorizaciones_salud` auts on siniestro.id_autorizacion=auts.id_autorizacion
left join (SELECT distinct cod_producto_AX,cod_producto FROM `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud_bitacora` ) doc on replace(siniestro.id_producto,'AX-','')=doc.cod_producto_AX
left join `{{project_id}}.tmp.carta_garantia_solicitud`  ctg on siniestro.id_siniestro=ctg.id_siniestro
left join  `{{project_id}}.tmp.maestro_cuenta_por_pagar` ob 
on SPLIT(ob.id_maestro_cuenta_por_pagar, '-')[OFFSET(0)] || '-' || SPLIT(ob.id_maestro_cuenta_por_pagar, '-')[OFFSET(1)]= coalesce(siniestro.num_obligacion)
where siniestro.periodo = DATE_TRUNC(current_date(),month) and  substr(siniestro.id_siniestro,4,1)<>'9'  and id_estado_siniestro_origen in ('4','8','T','5') and tip_reclamo ='C'

);



--- INFORMACIÓN ESTRUCTURADA DE OCR


create or replace table `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna` 
as (

select 
concat(num_factura_documento_ocr,ruc_proveedor_emisor_ocr) id,
num_factura_documento_ocr, --factura
coalesce(num_documento_paciente_siteds_ocr_val,dni_factura_ocr) dni_factura_ocr, --paciente
ruc_proveedor_emisor_ocr, --proveedor
poliza_factura_ocr, --cliente
fecha_emision_fact_limpio_val,
coalesce(cod_autorizacion_factura_ocr,num_siteds_ocr_val) num_siteds_ocr_val,
cod_autorizacion_liqui_ocr,
--cobertura_siteds_ocr,	
cobertura_siteds_ocr_limpia,	
producto_siteds_ocr,	
num_documento_paciente_siteds_ocr_val,
fecha_emision_siteds_ocr, --fecha atencion +/-5
num_poliza_siteds_ocr,
ruc_compania_factura_ocr,
flag_compania_factura_ocr AS cia, --compania
monto_sub_factura_ocr,
monto_factura_ocr, --monto 
flag_sinSiteds,
cie10_siteds_ocr, --dx
cod_cie101_trama_sited,
processed_date, 
case 
  when mecanismo_liqui_ocr='PPS' then '01' 
  when mecanismo_liqui_ocr='CPM' then '02' 
end mecanismo_liqui_ocr,
mecanismo_pago_fact_trama,
codigoproducto_trama,
nro_carta,
concat('25-',IF(ARRAY_LENGTH(SPLIT(nro_carta, '-')) = 2,
     SAFE_CAST(SUBSTR(SPLIT(nro_carta, '-')[OFFSET(0)], 2) AS INT64),
     NULL)) AS numero_carta_garantia,
  IF(ARRAY_LENGTH(SPLIT(nro_carta, '-')) = 2,
     SAFE_CAST(SPLIT(nro_carta, '-')[OFFSET(1)] AS INT64),
     NULL) AS version,
from `{{project_id}}.siniestro_salud_auna.auna_ocr_factura_siteds_consolidado_of_oficial`
QUALIFY
ROW_NUMBER() OVER (PARTITION BY num_factura_documento_ocr ORDER BY 
fecha_emision_siteds_ocr IS NULL,fecha_emision_siteds_ocr asc) = 1

);



-------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------- INICIO DE REGLAS DE DUPLICIDAD POR IGUALDAD DE DATOS  ---------------------------------
-------------------------------------------------------------------------------------------------------------------------------------

--- REGLAS FLUJO REGULAR 


  create or replace table `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_1` 
  as 
  with _tmp00 as 
    (
      select  distinct factura,id_persona_proveedor_siniestro,id_persona_afiliado,id_contratante,id_producto,id_cobertura_origen,num_diagnostico_origen,id_autorizacion,cod_mecanismo_pago,
      num_documento_proveedor_siniestro,fec_ult_liquidacion,
      fec_hora_ocurrencia,est_cuenta_por_pagar,id_estado_siniestro_origen,des_estado_siniestro_origen,
      num_siniestro,imp_documento,num_documento_afiliado,num_poliza,cod_producto,carta_garantia,
      from  `{{project_id}}.siniestro_salud_auna.tmp_rpt_anl_procesadas` where id_estado_siniestro_origen in ('4','8','T') 
    ) ,
     _tmp001 as 
     (
        select distinct a.id,auts.cod_autorizacion,a.monto_factura_ocr,a.mecanismo_pago_fact_trama,auts.cod_producto_ax,auts.cod_proveedor_rs,auts.cod_sede_proveedor,auts.cod_contratante_origen_sistema,
        replace(auts.id_persona_afiliado,'AX-','')  id_persona_afiliado,auts.cod_cobertura,a.fecha_emision_siteds_ocr fec_autorizacion, auts.fec_autorizacion fec_autorizacion_RS ,cie10_siteds_ocr, cod_cie101_trama_sited, 
        a.processed_date, a.dni_factura_ocr,a.num_poliza_siteds_ocr,a.ruc_proveedor_emisor_ocr,a.mecanismo_liqui_ocr,a.codigoproducto_trama,a.numero_carta_garantia,a.version ,
        from  `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna`  a inner join 
        `{{anl_project_id}}.anl_siniestro.autorizaciones_salud` auts on a.num_siteds_ocr_val=auts.cod_autorizacion
     )
    
    select distinct ocr.id,
     cast(_tmp00.fec_hora_ocurrencia as date) fec_ocurrencia,cast(_tmp00.fec_ult_liquidacion as date) fec_liquidacion,
     _tmp00.est_cuenta_por_pagar,_tmp00.id_estado_siniestro_origen,_tmp00.des_estado_siniestro_origen,_tmp00.num_siniestro,_tmp00.factura,_tmp00.imp_documento,
    case when 
      ocr.cod_autorizacion=_tmp00.id_autorizacion
      and  ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado          
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)      
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
      then 1 end flag_dup,
    case when 
      ocr.cod_autorizacion=_tmp00.id_autorizacion
      and  ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado          
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)      
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_observada,
    case when  
      ocr.cod_autorizacion=_tmp00.id_autorizacion
      and  ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado          
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)      
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_devolucion,
    case when 
      ocr.cod_autorizacion=_tmp00.id_autorizacion
      and  ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado          
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)      
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    then 'SITED_regla1: Existe Factura pagada por el proveedor con la misma atención en Sited, paciente, monto y ocurrencia' end dsc_dup     
     from _tmp001 ocr 
    join _tmp00 
      on ocr.cod_autorizacion=_tmp00.id_autorizacion
      and  ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado          
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)      
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
   ;
  


  create or replace table `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_2` 
  as 
  with _tmp00 as 
    (
      select  distinct factura,id_persona_proveedor_siniestro,id_persona_afiliado,id_contratante,id_producto,id_cobertura_origen,num_diagnostico_origen,id_autorizacion,cod_mecanismo_pago,
      num_documento_proveedor_siniestro,fec_ult_liquidacion,
      fec_hora_ocurrencia,est_cuenta_por_pagar,id_estado_siniestro_origen,des_estado_siniestro_origen,
      num_siniestro,imp_documento,num_documento_afiliado,num_poliza,cod_producto,carta_garantia,
      from  `{{project_id}}.siniestro_salud_auna.tmp_rpt_anl_procesadas` where id_estado_siniestro_origen in ('4','8','T') 
    ) ,
     _tmp001 as 
     (
        select  distinct a.id,auts.cod_autorizacion,a.monto_factura_ocr,a.mecanismo_pago_fact_trama,auts.cod_producto_ax,auts.cod_proveedor_rs,auts.cod_sede_proveedor,auts.cod_contratante_origen_sistema,
        replace(auts.id_persona_afiliado,'AX-','')  id_persona_afiliado,auts.cod_cobertura,a.fecha_emision_siteds_ocr fec_autorizacion, auts.fec_autorizacion fec_autorizacion_RS ,cie10_siteds_ocr, cod_cie101_trama_sited, 
        a.processed_date, a.dni_factura_ocr,a.num_poliza_siteds_ocr,a.ruc_proveedor_emisor_ocr,a.mecanismo_liqui_ocr,a.codigoproducto_trama,a.numero_carta_garantia,a.version ,      
        from  `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna`  a inner join 
        `{{anl_project_id}}.anl_siniestro.autorizaciones_salud` auts on cast(a.num_siteds_ocr_val as string)=auts.cod_autorizacion
     )
    
    select distinct ocr.id,
     cast(_tmp00.fec_hora_ocurrencia as date) fec_ocurrencia,cast(_tmp00.fec_ult_liquidacion as date) fec_liquidacion,
     _tmp00.est_cuenta_por_pagar,_tmp00.id_estado_siniestro_origen,_tmp00.des_estado_siniestro_origen,_tmp00.num_siniestro,_tmp00.factura,_tmp00.imp_documento,
    case when 
      ocr.cod_autorizacion=_tmp00.id_autorizacion      
      and trunc(_tmp00.imp_documento,0) between  trunc(ocr.monto_factura_ocr,0)-50 and trunc(ocr.monto_factura_ocr,0)+50    
      and _tmp00.cod_mecanismo_pago='01'
      and cast(ocr.mecanismo_pago_fact_trama as string)='02'
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
      then 1 end flag_dup,
    case when 
      ocr.cod_autorizacion=_tmp00.id_autorizacion      
      and trunc(_tmp00.imp_documento,0) between  trunc(ocr.monto_factura_ocr,0)-50 and trunc(ocr.monto_factura_ocr,0)+50    
      and _tmp00.cod_mecanismo_pago='01'
      and cast(ocr.mecanismo_pago_fact_trama as string)='02'
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_observada,
    case when  
      ocr.cod_autorizacion=_tmp00.id_autorizacion      
      and trunc(_tmp00.imp_documento,0) between  trunc(ocr.monto_factura_ocr,0)-50 and trunc(ocr.monto_factura_ocr,0)+50    
      and _tmp00.cod_mecanismo_pago='01'
      and cast(ocr.mecanismo_pago_fact_trama as string)='02'
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_devolucion,
    case when 
      ocr.cod_autorizacion=_tmp00.id_autorizacion      
      and trunc(_tmp00.imp_documento,0) between  trunc(ocr.monto_factura_ocr,0)-50 and trunc(ocr.monto_factura_ocr,0)+50    
      and _tmp00.cod_mecanismo_pago='01'
      and cast(ocr.mecanismo_pago_fact_trama as string)='02'
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    then 'SITED_regla2: Existe Factura pagada por el proveedor con el mismo Sited y ocurrencia por PPS que debio facturarse en CPM' end dsc_dup     
     from _tmp001 ocr 
    join _tmp00 
      on ocr.cod_autorizacion=_tmp00.id_autorizacion      
      and trunc(_tmp00.imp_documento,0) between  trunc(ocr.monto_factura_ocr,0)-50 and trunc(ocr.monto_factura_ocr,0)+50    
      and _tmp00.cod_mecanismo_pago='01'
      and cast(ocr.mecanismo_pago_fact_trama as string)='02'
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
      where ocr.id not in ( select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_1` )
   ;


    
  create or replace table `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_3` 
  as 
  with _tmp00 as 
    (
      select  distinct factura,id_persona_proveedor_siniestro,id_persona_afiliado,id_contratante,id_producto,id_cobertura_origen,num_diagnostico_origen,id_autorizacion,cod_mecanismo_pago,
      num_documento_proveedor_siniestro,fec_ult_liquidacion,
      fec_hora_ocurrencia,est_cuenta_por_pagar,id_estado_siniestro_origen,des_estado_siniestro_origen,
      num_siniestro,imp_documento,num_documento_afiliado,num_poliza,cod_producto,carta_garantia,
      from  `{{project_id}}.siniestro_salud_auna.tmp_rpt_anl_procesadas` where id_estado_siniestro_origen in ('4','8','T') 
    )
    select distinct ocr.id,
     cast(_tmp00.fec_hora_ocurrencia as date) fec_ocurrencia,cast(_tmp00.fec_ult_liquidacion as date) fec_liquidacion,
     _tmp00.est_cuenta_por_pagar,_tmp00.id_estado_siniestro_origen,_tmp00.des_estado_siniestro_origen,_tmp00.num_siniestro,_tmp00.factura,_tmp00.imp_documento,
    case when   ocr.num_factura_documento_ocr =  _tmp00.factura and ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro
    and cast(ocr.processed_date as date)<=cast(_tmp00.fec_ult_liquidacion as date)  then 1 end flag_dup,
    case when  ocr.num_factura_documento_ocr =  _tmp00.factura and ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro
    and cast(ocr.processed_date as date)<=cast(_tmp00.fec_ult_liquidacion as date) then 0 end flag_observada,
    case when   ocr.num_factura_documento_ocr =  _tmp00.factura and ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro
    and cast(ocr.processed_date as date)<=cast(_tmp00.fec_ult_liquidacion as date) then 0 end flag_devolucion,
    case when   ocr.num_factura_documento_ocr =  _tmp00.factura and ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro
    and cast(ocr.processed_date as date)<=cast(_tmp00.fec_ult_liquidacion as date) then concat('FACTURA_regla3: Existe Factura pagada, proveedor utilizo la factura con el monto ',_tmp00.imp_documento) end dsc_dup
     from `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna` ocr 
    join _tmp00 _tmp00 on ocr.num_factura_documento_ocr =  _tmp00.factura and ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro
    and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    where ocr.id not in ( 
      select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_1` union all
      select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_2`
    ) ;
  


  create or replace table `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_4` 
  as 
  with _tmp00 as 
    (
      select  distinct factura,id_persona_proveedor_siniestro,id_persona_afiliado,id_contratante,id_producto,id_cobertura_origen,num_diagnostico_origen,id_autorizacion,cod_mecanismo_pago,
      num_documento_proveedor_siniestro,fec_ult_liquidacion,
      fec_hora_ocurrencia,est_cuenta_por_pagar,id_estado_siniestro_origen,des_estado_siniestro_origen,
      num_siniestro,imp_documento,num_documento_afiliado,num_poliza,cod_producto,carta_garantia,
      from  `{{project_id}}.siniestro_salud_auna.tmp_rpt_anl_procesadas` where id_estado_siniestro_origen in ('4','8','T') 
    ) ,
     _tmp001 as 
     (
        select distinct  a.id,auts.cod_autorizacion,a.monto_factura_ocr,a.mecanismo_pago_fact_trama,auts.cod_producto_ax,auts.cod_proveedor_rs,auts.cod_sede_proveedor,auts.cod_contratante_origen_sistema,
        replace(auts.id_persona_afiliado,'AX-','')  id_persona_afiliado,auts.cod_cobertura,a.fecha_emision_siteds_ocr fec_autorizacion, auts.fec_autorizacion fec_autorizacion_RS ,cie10_siteds_ocr, cod_cie101_trama_sited, 
        a.processed_date, a.dni_factura_ocr,a.num_poliza_siteds_ocr,a.ruc_proveedor_emisor_ocr,a.mecanismo_liqui_ocr,a.codigoproducto_trama,a.numero_carta_garantia,a.version ,
        from  `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna`  a inner join 
        `{{anl_project_id}}.anl_siniestro.autorizaciones_salud` auts on a.num_siteds_ocr_val=auts.cod_autorizacion
     )
    
    select distinct ocr.id,
     cast(_tmp00.fec_hora_ocurrencia as date) fec_ocurrencia,cast(_tmp00.fec_ult_liquidacion as date) fec_liquidacion,
     _tmp00.est_cuenta_por_pagar,_tmp00.id_estado_siniestro_origen,_tmp00.des_estado_siniestro_origen,_tmp00.num_siniestro,_tmp00.factura,_tmp00.imp_documento,
    case when 
      ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante       
      and  _tmp00.cod_mecanismo_pago='02' 
      and ocr.mecanismo_pago_fact_trama='02'
      and cast(format_date('%Y%m',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
      then 1 end flag_dup,
    case when 
      ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante       
      and  _tmp00.cod_mecanismo_pago='02' 
      and ocr.mecanismo_pago_fact_trama='02'
      and cast(format_date('%Y%m',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_observada,
    case when  
      ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante       
      and  _tmp00.cod_mecanismo_pago='02' 
      and ocr.mecanismo_pago_fact_trama='02'
      and cast(format_date('%Y%m',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_devolucion,
    case when 
      ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante       
      and  _tmp00.cod_mecanismo_pago='02' 
      and ocr.mecanismo_pago_fact_trama='02'
      and cast(format_date('%Y%m',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    then 'ATENCION_regla4: Existe Factura pagada por el proveedor para la misma atención en CPM' end dsc_dup     
     from _tmp001 ocr 
    join _tmp00 
      on   ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante       
      and  _tmp00.cod_mecanismo_pago='02' 
      and ocr.mecanismo_pago_fact_trama='02'
      and cast(format_date('%Y%m',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m',_tmp00.fec_hora_ocurrencia) as string)   
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
      where ocr.id not in 
      ( 
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_1` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_2` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_3` 
       )
   ;




  create or replace table `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_5` 
  as 
  with _tmp00 as 
    (
      select  distinct factura,id_persona_proveedor_siniestro,id_persona_afiliado,id_contratante,id_producto,id_cobertura_origen,num_diagnostico_origen,id_autorizacion,cod_mecanismo_pago,
      num_documento_proveedor_siniestro,fec_ult_liquidacion,
      fec_hora_ocurrencia,est_cuenta_por_pagar,id_estado_siniestro_origen,des_estado_siniestro_origen,
      num_siniestro,imp_documento,num_documento_afiliado,num_poliza,cod_producto,carta_garantia,
      from  `{{project_id}}.siniestro_salud_auna.tmp_rpt_anl_procesadas` where id_estado_siniestro_origen in ('4','8','T') 
    ) ,
     _tmp001 as 
     (
        select distinct  a.id,auts.cod_autorizacion,a.monto_factura_ocr,a.mecanismo_pago_fact_trama,auts.cod_producto_ax,auts.cod_proveedor_rs,auts.cod_sede_proveedor,auts.cod_contratante_origen_sistema,
        replace(auts.id_persona_afiliado,'AX-','')  id_persona_afiliado,auts.cod_cobertura,a.fecha_emision_siteds_ocr fec_autorizacion, auts.fec_autorizacion fec_autorizacion_RS ,cie10_siteds_ocr, cod_cie101_trama_sited, 
        a.processed_date, a.dni_factura_ocr,a.num_poliza_siteds_ocr,a.ruc_proveedor_emisor_ocr,a.mecanismo_liqui_ocr,a.codigoproducto_trama,a.numero_carta_garantia,a.version ,
        from  `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna`  a inner join 
        `{{anl_project_id}}.anl_siniestro.autorizaciones_salud` auts on a.num_siteds_ocr_val=auts.cod_autorizacion
     ),

    _tmp002 as
    ( 
    select distinct ocr.id,
     cast(_tmp00.fec_hora_ocurrencia as date) fec_ocurrencia,cast(_tmp00.fec_ult_liquidacion as date) fec_liquidacion,
     _tmp00.est_cuenta_por_pagar,_tmp00.id_estado_siniestro_origen,_tmp00.des_estado_siniestro_origen,_tmp00.num_siniestro,_tmp00.factura,_tmp00.imp_documento,
    case when 
       ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)       
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)  
       then 1 end flag_dup,
    case when 
      ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)       
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_observada,
    case when  
      ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)       
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_devolucion,
    case when 
       ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)       
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    then 'ATENCION_regla5: Existe Factura pagada por el proveedor con la misma atención en paciente, producto, cliente, cobertura, diagnóstico, monto y misma fecha de emisión del Sited' end dsc_dup     
     from _tmp001 ocr 
    join _tmp00 
      on  ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)       
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    where  ocr.id not in ( 
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_1` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_2` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_3`  union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_4` 
       )
    )
      select _tmp002.* from _tmp002 
      where id not in (
        select distinct id from ( select id,count(1) q from _tmp002 group by all having count(1)>1 )
      )
      
    ;
  


  create or replace table `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_6` 
  as 
  with _tmp00 as 
    (
      select  distinct factura,id_persona_proveedor_siniestro,id_persona_afiliado,id_contratante,id_producto,id_cobertura_origen,num_diagnostico_origen,id_autorizacion,cod_mecanismo_pago,
      num_documento_proveedor_siniestro,fec_ult_liquidacion,
      fec_hora_ocurrencia,est_cuenta_por_pagar,id_estado_siniestro_origen,des_estado_siniestro_origen,
      num_siniestro,imp_documento,num_documento_afiliado,num_poliza,cod_producto,carta_garantia,
      from  `{{project_id}}.siniestro_salud_auna.tmp_rpt_anl_procesadas` where id_estado_siniestro_origen in ('4','8','T') 
    ) ,
     _tmp001 as 
     (
        select distinct  a.id,auts.cod_autorizacion,a.monto_factura_ocr,a.mecanismo_pago_fact_trama,auts.cod_producto_ax,auts.cod_proveedor_rs,auts.cod_sede_proveedor,auts.cod_contratante_origen_sistema,
        replace(auts.id_persona_afiliado,'AX-','')  id_persona_afiliado,auts.cod_cobertura,a.fecha_emision_siteds_ocr fec_autorizacion, auts.fec_autorizacion fec_autorizacion_RS ,cie10_siteds_ocr, cod_cie101_trama_sited, 
        a.processed_date, a.dni_factura_ocr,a.num_poliza_siteds_ocr,a.ruc_proveedor_emisor_ocr,a.mecanismo_liqui_ocr,a.codigoproducto_trama,a.numero_carta_garantia,a.version ,
        from  `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna`  a left join 
        `{{anl_project_id}}.anl_siniestro.autorizaciones_salud` auts on a.num_siteds_ocr_val=auts.cod_autorizacion
     ),

    _tmp002 as
    ( 
    select distinct ocr.id,
     cast(_tmp00.fec_hora_ocurrencia as date) fec_ocurrencia,cast(_tmp00.fec_ult_liquidacion as date) fec_liquidacion,
     _tmp00.est_cuenta_por_pagar,_tmp00.id_estado_siniestro_origen,_tmp00.des_estado_siniestro_origen,_tmp00.num_siniestro,_tmp00.factura,_tmp00.imp_documento,
    case when 
        ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)        
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)    
       then 1 end flag_dup,
    case when 
        ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)        
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_observada,
    case when  
        ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)        
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_devolucion,
    case when 
         ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)        
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    then 'ATENCION_OCR_regla6: Existe Factura pagada por el proveedor con la misma atención en paciente, producto, cliente,  diagnóstico, monto y misma fecha de la emisión del Sited' end dsc_dup     
     from _tmp001 ocr 
    join _tmp00 
      on  ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(format_date('%Y%m%d',ocr.fec_autorizacion_RS) as string) = cast(format_date('%Y%m%d',_tmp00.fec_hora_ocurrencia) as string)        
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    where  ocr.id not in ( 
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_1` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_2` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_3` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_4` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_5` 
       )
    ) 
      select _tmp002.* from _tmp002 
      where id not in (
        select distinct id from ( select id,count(1) q from _tmp002 group by all having count(1)>1 )
      )
      
    ;

        

  create or replace table `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_7` 
  as 
  with _tmp00 as 
    (
      select  distinct factura,id_persona_proveedor_siniestro,id_persona_afiliado,id_contratante,id_producto,id_cobertura_origen,num_diagnostico_origen,id_autorizacion,cod_mecanismo_pago,
      num_documento_proveedor_siniestro,fec_ult_liquidacion,
      fec_hora_ocurrencia,est_cuenta_por_pagar,id_estado_siniestro_origen,des_estado_siniestro_origen,
      num_siniestro,imp_documento,num_documento_afiliado,num_poliza,cod_producto,carta_garantia,
      from  `{{project_id}}.siniestro_salud_auna.tmp_rpt_anl_procesadas` where id_estado_siniestro_origen in ('4','8','T') 
    ) ,
     _tmp001 as 
     (
        select distinct  a.id,auts.cod_autorizacion,a.monto_factura_ocr,a.mecanismo_pago_fact_trama,auts.cod_producto_ax,auts.cod_proveedor_rs,auts.cod_sede_proveedor,auts.cod_contratante_origen_sistema,
        replace(auts.id_persona_afiliado,'AX-','')  id_persona_afiliado,auts.cod_cobertura,a.fecha_emision_siteds_ocr fec_autorizacion, auts.fec_autorizacion fec_autorizacion_RS ,cie10_siteds_ocr, cod_cie101_trama_sited, 
        a.processed_date, a.dni_factura_ocr,a.num_poliza_siteds_ocr,a.ruc_proveedor_emisor_ocr,a.mecanismo_liqui_ocr,a.codigoproducto_trama,a.numero_carta_garantia,a.version ,
        from  `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna`  a left join 
        `{{anl_project_id}}.anl_siniestro.autorizaciones_salud` auts on a.num_siteds_ocr_val=auts.cod_autorizacion
     ),

    _tmp002 as
    ( 
    select distinct ocr.id,
     cast(_tmp00.fec_hora_ocurrencia as date) fec_ocurrencia,cast(_tmp00.fec_ult_liquidacion as date) fec_liquidacion,
     _tmp00.est_cuenta_por_pagar,_tmp00.id_estado_siniestro_origen,_tmp00.des_estado_siniestro_origen,_tmp00.num_siniestro,_tmp00.factura,_tmp00.imp_documento,
    case when 
       ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and ocr.numero_carta_garantia = _tmp00.carta_garantia 
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY)
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)    
       then 1 end flag_dup,
    case when 
       ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and ocr.numero_carta_garantia = _tmp00.carta_garantia 
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY)
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_observada,
    case when  
       ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and ocr.numero_carta_garantia = _tmp00.carta_garantia 
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY)
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
     then 0 end flag_devolucion,
    case when 
        ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and ocr.numero_carta_garantia = _tmp00.carta_garantia 
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY)
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    then 'ATENCION_OCR_regla7: Existe Factura pagada por el proveedor con la misma atención en paciente, producto, cliente,  diagnóstico, monto, carta de garantia y por aproximación en 5 días de la ocurrencia desde la emisión del Sited' end dsc_dup     
     from _tmp001 ocr 
    join _tmp00 
      on  ocr.ruc_proveedor_emisor_ocr=_tmp00.num_documento_proveedor_siniestro 
      and ocr.dni_factura_ocr=_tmp00.num_documento_afiliado 
      and ocr.num_poliza_siteds_ocr=_tmp00.num_poliza
      and ocr.codigoproducto_trama=_tmp00.cod_producto      
      and ocr.cie10_siteds_ocr = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and ocr.numero_carta_garantia = _tmp00.carta_garantia 
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY)
      and cast(ocr.processed_date as date)>=cast(_tmp00.fec_ult_liquidacion as date)
    where  ocr.id not in ( 
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_1` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_2` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_3` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_4` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_5` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_6` 
       )
    ) 
    select * from _tmp002 
     
    ;



  create or replace table `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_8` 
  as 
  with _tmp00 as 
    (
      select  distinct factura,id_persona_proveedor_siniestro,id_persona_afiliado,id_contratante,id_producto,id_cobertura_origen,num_diagnostico_origen,id_autorizacion,cod_mecanismo_pago,
      num_documento_proveedor_siniestro,fec_ult_liquidacion,
      fec_hora_ocurrencia,est_cuenta_por_pagar,id_estado_siniestro_origen,des_estado_siniestro_origen,
      num_siniestro,imp_documento,num_documento_afiliado,num_poliza,cod_producto,carta_garantia,
      from  `{{project_id}}.siniestro_salud_auna.tmp_rpt_anl_procesadas` where id_estado_siniestro_origen = '5' and ind_devoluc = 'Rechazo_Definitivo'
    ) ,
     _tmp001 as 
     (
        select distinct  a.id,auts.cod_autorizacion,a.monto_factura_ocr,a.mecanismo_pago_fact_trama,auts.cod_producto_ax,auts.cod_proveedor_rs,auts.cod_sede_proveedor,auts.cod_contratante_origen_sistema,
        replace(auts.id_persona_afiliado,'AX-','')  id_persona_afiliado,auts.cod_cobertura,a.fecha_emision_siteds_ocr fec_autorizacion, auts.fec_autorizacion fec_autorizacion_RS ,cie10_siteds_ocr, cod_cie101_trama_sited, 
        a.processed_date, a.dni_factura_ocr,a.num_poliza_siteds_ocr,a.ruc_proveedor_emisor_ocr,a.mecanismo_liqui_ocr,a.codigoproducto_trama,a.numero_carta_garantia,a.version ,
        from  `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna`  a left join 
        `{{anl_project_id}}.anl_siniestro.autorizaciones_salud` auts on a.num_siteds_ocr_val=auts.cod_autorizacion
     ),

    _tmp002 as
    ( 
    select distinct ocr.id,
     cast(_tmp00.fec_hora_ocurrencia as date) fec_ocurrencia,cast(_tmp00.fec_ult_liquidacion as date) fec_liquidacion,
     _tmp00.est_cuenta_por_pagar,_tmp00.id_estado_siniestro_origen,_tmp00.des_estado_siniestro_origen,_tmp00.num_siniestro,_tmp00.factura,_tmp00.imp_documento,
    case when 
      ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY)    
       then 0 end flag_dup,
    case when 
      ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY) 
     then 0 end flag_observada,
    case when  
      ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY) 
     then 1 end flag_devolucion,
    case when 
       ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY) 
    then 'ATENCION_regla9: Factura con rechazo definito que no debe volver a ingresar' end dsc_dup     
     from _tmp001 ocr 
    join _tmp00 
      on  ocr.cod_proveedor_rs=_tmp00.id_persona_proveedor_siniestro 
      and ocr.id_persona_afiliado=_tmp00.id_persona_afiliado 
      and ocr.cod_contratante_origen_sistema=_tmp00.id_contratante
      and ocr.cod_producto_ax=_tmp00.id_producto
      and ocr.cod_cobertura=_tmp00.id_cobertura_origen
      and ocr.cod_cie101_trama_sited = _tmp00.num_diagnostico_origen   
      and trunc(ocr.monto_factura_ocr,0) = trunc(_tmp00.imp_documento,0)
      and cast(_tmp00.fec_hora_ocurrencia as date) between DATE_SUB(cast(ocr.fec_autorizacion as date), INTERVAL 5 DAY)  and  DATE_ADD(cast(ocr.fec_autorizacion_RS as date), INTERVAL 5 DAY)      
    where  ocr.id not in ( 
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_1` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_2` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_3` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_4` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_5` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_6` union all
        select distinct id from `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_7` 
       )
    ) 
    select * from _tmp002 
     
    ;




--- UNIFICACIÓN DE REGLAS ENCONTRADAS


create or replace table `{{project_id}}.siniestro_salud_auna.pdfs_pre_auna_fr_reglas`
as 
(
select distinct 
concat(a.num_factura_documento_ocr,a.ruc_proveedor_emisor_ocr) id,a.num_factura_documento_ocr,

coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.flag_dup,r2.flag_dup),r3.flag_dup),r4.flag_dup),r5.flag_dup),r6.flag_dup),r7.flag_dup),r8.flag_dup),0) flag_dup, 

coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.flag_observada,r2.flag_observada),r3.flag_observada),r4.flag_observada),r5.flag_observada),r6.flag_observada),r7.flag_observada),r8.flag_observada),0) flag_observada,

coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.flag_devolucion,r2.flag_devolucion),r3.flag_devolucion),r4.flag_devolucion),r5.flag_devolucion),r6.flag_devolucion),r7.flag_devolucion),r8.flag_devolucion),0) flag_devolucion,

coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.fec_ocurrencia,r2.fec_ocurrencia),r3.fec_ocurrencia),r4.fec_ocurrencia),r5.fec_ocurrencia),r6.fec_ocurrencia),r7.fec_ocurrencia),r8.fec_ocurrencia),null) fec_atencion,

coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.fec_liquidacion,r2.fec_liquidacion),r3.fec_liquidacion),r4.fec_liquidacion),r5.fec_liquidacion),r6.fec_liquidacion),r7.fec_liquidacion),r8.fec_liquidacion),null) fec_liquidacion,

coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.num_siniestro,r2.num_siniestro),r3.num_siniestro),r4.num_siniestro),r5.num_siniestro),r6.num_siniestro),r7.num_siniestro),r8.num_siniestro),null) num_siniestro,

coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.factura,r2.factura),r3.factura),r4.factura),r5.factura),r6.factura),r7.factura),r8.factura),null)  factura_duplicada,
coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.imp_documento,r2.imp_documento),r3.imp_documento),r4.imp_documento),r5.imp_documento),r6.imp_documento),r7.imp_documento),r8.imp_documento),null) monto_duplicado,
coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.des_estado_siniestro_origen,r2.des_estado_siniestro_origen),r3.des_estado_siniestro_origen),r4.des_estado_siniestro_origen),r5.des_estado_siniestro_origen),r6.des_estado_siniestro_origen),r7.des_estado_siniestro_origen),r8.des_estado_siniestro_origen),null) estado_procesado,
coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.dsc_dup,r2.dsc_dup),r3.dsc_dup),r4.dsc_dup),r5.dsc_dup),r6.dsc_dup),r7.dsc_dup),r8.dsc_dup),null) dsc_dup,
coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(r1.est_cuenta_por_pagar,r2.est_cuenta_por_pagar),r3.est_cuenta_por_pagar),r4.est_cuenta_por_pagar),r5.est_cuenta_por_pagar),r6.est_cuenta_por_pagar),r7.est_cuenta_por_pagar),r8.est_cuenta_por_pagar),null) sts_abono

  from  `{{project_id}}.siniestro_salud_auna._tmp_pfs_auna` a
left join `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_1` r1 on concat(a.num_factura_documento_ocr,a.ruc_proveedor_emisor_ocr)=r1.id
left join `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_2` r2 on concat(a.num_factura_documento_ocr,a.ruc_proveedor_emisor_ocr)=r2.id
left join `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_3` r3 on concat(a.num_factura_documento_ocr,a.ruc_proveedor_emisor_ocr)=r3.id
left join `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_4` r4 on concat(a.num_factura_documento_ocr,a.ruc_proveedor_emisor_ocr)=r4.id
left join `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_5` r5 on concat(a.num_factura_documento_ocr,a.ruc_proveedor_emisor_ocr)=r5.id
left join `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_6` r6 on concat(a.num_factura_documento_ocr,a.ruc_proveedor_emisor_ocr)=r6.id
left join `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_7` r7 on concat(a.num_factura_documento_ocr,a.ruc_proveedor_emisor_ocr)=r7.id
left join `{{project_id}}.siniestro_salud_auna._tmp_pfs_regla_fregular_8` r8 on concat(a.num_factura_documento_ocr,a.ruc_proveedor_emisor_ocr)=r8.id
      ) ;
    


--tabla final


create or replace table `{{project_id}}.siniestro_salud_auna.pdfs_auna_reglas`
as 
(
  select distinct
  *, case when sts_abono='PAG' then 1 else 0 end flag_abono, case when sts_abono='PAG' then 'Pagado' when sts_abono in ('OPT','PGP') then 'Pendiente Pago'  end dsc_abono
  from `{{project_id}}.siniestro_salud_auna.pdfs_pre_auna_fr_reglas`
  where  num_factura_documento_ocr<>factura_duplicada  and flag_dup=1 
  QUALIFY ROW_NUMBER() OVER(partition by id ORDER BY fec_atencion DESC) = 1
  union all
  select distinct
  *,0, ''
  from `{{project_id}}.siniestro_salud_auna.pdfs_pre_auna_fr_reglas`
  where  flag_devolucion=1

) ;

