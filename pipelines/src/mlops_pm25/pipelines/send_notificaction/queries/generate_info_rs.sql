create or replace table `{{project_id}}.siniestro_salud_auna.auna_facturas_RS_oficial`
as 

with _tmp00 as 
(
select 
case 
when id_estado_siniestro_origen in ('4','8','T') then 'Liquidado'
  when id_estado_siniestro_origen in ('D','R','E','3','2','9','1','V','0') then 'Reserva'
  else 'Rechazos' end  tipo_base, 
 cast(fec_notificacion as date) fecha_recepcion,
 cast(siniestro.fec_ult_liquidacion as date) fec_liquidacion,
 cast(siniestro.fec_hora_ocurrencia as date) fec_ocurrencia,
 case 
 when substr(id_siniestro_origen,1,1) ='1' then 'EPS'
 when substr(id_siniestro_origen,1,1) ='2' then 'ASISTENCIA MEDICA' end compania,
 siniestro.num_siniestro,
 concat(cod_serie_comprobante_siniestro,lpad(num_comprobante_siniestro,8,'0')) as factura,
 nom_completo_proveedor_siniestro proveedor,nom_sede_proveedor_siniestro sede,nom_completo_afiliado paciente,
 case when id_estado_siniestro_origen in ('4','8') then bef_dx.des_cobertura else c.des_cobertura end des_cobertura,
 des_estado_siniestro_origen desc_estado,
 nom_completo_contratante,
 des_producto,
 des_producto_agrupado,
case 
  when ind_trama_tedef='SI' then 'TRAMA'
  when ind_trama_tedef='NO' then 'MANUAL'
  end tipo_ingreso,
  mto_auditado_sol,
  mnt_pagar_base,
  tip_caso_especial,
sum(ats.mnt_beneficio_sin_impuesto_aprobado_sol) mnt_beneficio_sin_impuesto_aprobado_sol,
sum(ats.mnt_beneficio_sin_impuesto_aprobado_sol + ats.mnt_impuesto_aprobado_sol) mnt_beneficio_con_impuesto_aprobado_sol
from `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud`  siniestro
left join unnest(atencion_salud) ats
left join 
(  
  select periodo,id_siniestro,num_diagnostico_origen,des_diagnostico_salud,id_cobertura_origen,des_cobertura,agrupacion_cobertura_negocio,agrupacion_cobertura_subnegocio,ind_covid,des_tipo_atencion
    from 
  (select distinct siniestro.periodo,siniestro.id_siniestro,
  ats.num_diagnostico_origen,
  ats.des_diagnostico_salud,ats.id_cobertura_origen,ats.des_cobertura,ats.num_correlativo_atencion,ats.agrupacion_cobertura_subnegocio,ats.agrupacion_cobertura_negocio,ind_covid,ats.des_tipo_atencion,
    row_number()over(partition by siniestro.id_siniestro order by ats.num_correlativo_atencion asc) x
    from `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud` siniestro left join unnest(atencion_salud) ats  
    where siniestro.periodo = DATE_TRUNC(current_date(),month) 
  ) where x=1
) bef_dx
on siniestro.id_siniestro=bef_dx.id_siniestro and siniestro.periodo=bef_dx.periodo
left join (
  select distinct 
id_siniestro,
num_siniestro,
dx.id_cobertura_principal id_cobertura_origen,  
dx.des_cobertura_principal des_cobertura,
dx.num_diagnostico_principal num_diagnostico_origen,
dx.des_diagnostico_principal des_diagnostico_salud,
ats.num_correlativo_atencion 
 from   `{{anl_project_id}}.anl_siniestro.siniestro_detalle_salud` siniestro 
left join unnest(atencion_salud) ats
left join unnest(diagnostico_principal) dx
 where  siniestro.periodo  =  DATE_TRUNC(current_date(),month) and  substr(id_siniestro,4,1)<>'9' 
 QUALIFY ROW_NUMBER() OVER(partition by num_siniestro ORDER BY ats.num_correlativo_atencion asc) = 1
 ) c on siniestro.id_siniestro=c.id_siniestro
 where siniestro.periodo = DATE_TRUNC(current_date(),month) and substr(siniestro.id_siniestro,4,1)<>'9' and cast(fec_notificacion as date) >='2025-01-20' 
 --and cast(format_date('%Y',fec_hora_ocurrencia) as int) >= 2025
 -- and id_persona_proveedor_siniestro in ('AX-1237',	'AX-629',	'AX-8920981',	'AX-8227559',	'AX-8637298',	'AX-276471',	'AX-116520',	'AX-422739') 
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
and tip_reclamo='C'
 --and (trim(tip_caso_especial)<>'CONCILIACIONES' OR trim(tip_caso_especial) is null)
 group by all
), 
  _tmp_bandejas as 
  (
      select distinct
      concat(substr(cast(NRO_SINIESTRO as string),1,2),'-',cast(cast(substr(cast(NRO_SINIESTRO as string),5,12)  as integer) as string)) num_siniestro,
      --concat(substr(cast(NRO_SINIESTRO as string),1,2),'-',cast(cast(substr(cast(NRO_SINIESTRO as string),5,12)  as integer) as string)) num_siniestro,
      case when REGEXP_CONTAINS(factura, r'-') THEN CONCAT(SUBSTR(factura, 1, 4),LPAD(SUBSTR(factura, STRPOS(factura, '-') + 1), 8, '0')) end factura,
      case 
        when trim(TAREA) in ('NOTIFICADOS','NOTIFICADO-CAMUNDA') then 'NOTIFICADOS'
        when trim(TAREA) ='' then  case when trim(DESC_ESTADO) = 'CALCULADO' then trim(DESC_ESTADO) else  trim(TIPO_SINIESTRO) end -- tipo no tengo, no va
        else trim(TAREA) 
      end bandeja,
      coalesce(trim(MOTIVO_NOTIFICADO),trim(MOTIVO_REPROCESO)) motivo_notificado,
      trim(DESC_BENEFICIO) DESC_BENEFICIO 
      FROM `{{project_id}}.siniestro_salud_auna.sb_cp_reserva_linea_credito_hist_dia`
  )
  select 
  'Regular' as flujo,tipo_base,bandeja,motivo_notificado,fecha_recepcion,fec_liquidacion,fec_ocurrencia,compania,a.num_siniestro,a.factura,'RED AUNA' red,proveedor,sede,paciente,coalesce(des_cobertura,DESC_BENEFICIO) des_cobertura,desc_estado,tipo_ingreso,tip_caso_especial,
  nom_completo_contratante,des_producto,des_producto_agrupado,
  coalesce(case when mnt_beneficio_con_impuesto_aprobado_sol=0 then null else mnt_beneficio_con_impuesto_aprobado_sol end,mto_auditado_sol) imp_pagar,
  coalesce(case when mnt_beneficio_sin_impuesto_aprobado_sol=0 then null else mnt_beneficio_sin_impuesto_aprobado_sol end,mnt_pagar_base) imp_pagar_base,
  --b.COD_MOTIVO_DEVOLUCION cod_motivo,b.des_motivo_devolucion, b.comentario_motivo_dev,b.ind_devolucion, b.FEC_DEVOLUCION fec_devolucion, b.fec_creacion_carta, b.observacion_carta,  
  cast(current_date("America/Lima") as date)-1 as fec_actualizacion
   from _tmp00 a 
   --left join `rs-prd-dlk-sbx-evco-d1a7.raw_eve_cobranzas._tmp_mov_siniestro_upd` b on a.num_siniestro=b.num_siniestro
   left join _tmp_bandejas _tmp_bandejas on a.num_siniestro=_tmp_bandejas.num_siniestro
