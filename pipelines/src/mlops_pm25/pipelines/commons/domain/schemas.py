from .constants import DocumentKeyEnum

direction_schema = {
        "name": "direccion", 
        "type": "RECORD",
        "fields": [
            {"name": "calle", "type": "STRING"},
            {"name": "distrito", "type": "STRING"},
            {"name": "provincia", "type": "STRING"},
            {"name": "departamento", "type": "STRING"}
        ]
    }

INVOICE_SCHEMA = [
    #{"name": "created_at", "type": "TIMESTAMP"},
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    {
        "name": "emisor", 
        "type": "RECORD",
        "fields": [
            {"name": "nombre", "type": "STRING"},
            {"name": "ruc", "type": "STRING"},
            direction_schema
        ]
    },
    {
        "name": "receptor", 
        "type": "RECORD",
        "fields": [
            {"name": "nombre_senior", "type": "STRING"},
            {"name": "ruc", "type": "STRING"},
            direction_schema
        ]
    },
    {
        "name": "paciente", 
        "type": "RECORD",
        "fields": [
            {"name": "nombre_completo", "type": "STRING"},
            {"name": "dni", "type": "STRING"},
            direction_schema,
            {"name": "titular", "type": "STRING"},
            {"name": "compania", "type": "STRING"},
            {"name": "codigo_autorizacion", "type": "STRING"},
            {"name": "carta", "type": "STRING"},
            {"name": "observacion", "type": "STRING"},
        ],
    },
    {
        "name": "cabecera", 
        "type": "RECORD",
        "fields": [
            {"name": "numero_factura", "type": "STRING"},
            {"name": "fecha_emision", "type": "STRING"}
        ],
    },
    {
        "name": "info_seguro", 
        "type": "RECORD",
        "fields": [
            {"name": "empresa", "type": "STRING"},
            {"name": "condiciones_pago", "type": "STRING"},
            {"name": "numero_expediente", "type": "STRING"},
            {"name": "historia_clinica_hc", "type": "STRING"},
            {"name": "encuentro", "type": "STRING"},
            {"name": "poliza", "type": "STRING"}
        ]            
    },
    {
        'name': 'items', 'type': 'RECORD', 'mode': 'REPEATED',
        'fields': [
            {'name': 'descripcion', 'type': 'STRING'},
            {'name': 'cantidad', 'type': 'NUMERIC'},
            {'name': 'unidad_medida', 'type': 'STRING'},
            {'name': 'precio_unitario', 'type': 'NUMERIC'},
            {'name': 'descuento', 'type': 'NUMERIC'},
            {'name': 'valor_venta', 'type': 'NUMERIC'}
    ]},

    {
        "name": "totales", 
        "type": "RECORD",
        "fields": [
            {"name": "icper", "type": "NUMERIC"},
            {"name": "igv", "type": "NUMERIC"},
            {"name": "importe_total", "type": "NUMERIC"},
            {"name": "operaciones_exoneradas", "type": "NUMERIC"},
            {"name": "operaciones_gratuitas", "type": "NUMERIC"},
            {"name": "operaciones_gravadas", "type": "NUMERIC"},
            {"name": "operaciones_inafectas", "type": "NUMERIC"},
            {"name": "subtotal", "type": "NUMERIC"},
            {"name": "total_descuentos", "type": "NUMERIC"},
            {"name": "total_venta", "type": "NUMERIC"}
        ],
    },
    {
        "name": "importe", 
        "type": "RECORD",
        "fields": [
            {"name": "monto", "type": "NUMERIC"},
            {"name": "tipo_moneda", "type": "STRING"}
        ]
    },
    {
        "name": "importe_letras", 
        "type": "STRING"
    }

]

SITED_SCHEMA = [
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    
    # Cabecera
    {
        "name": "cabecera", 
        "type": "RECORD",
        "fields": [
            {"name": "numero_orden", "type": "STRING"},
            {"name": "producto", "type": "STRING"}
        ]
    },
    
    # Paciente
    {
        "name": "paciente", 
        "type": "RECORD",
        "fields": [
            {"name": "nombre_completo", "type": "STRING"},
            {"name": "parentesco", "type": "STRING"},
            {"name": "sexo", "type": "STRING"},
            {"name": "edad", "type": "NUMERICEGER"},
            {"name": "tipo_documento", "type": "STRING"},
            {"name": "num_documento", "type": "STRING"},
            {"name": "inicio_vigencia", "type": "STRING"},
            {"name": "num_solicitud_origen", "type": "STRING"},
            {"name": "fin_vigencia", "type": "STRING"},
            {"name": "fecha_nacimiento", "type": "STRING"},
            {"name": "num_decl_accidente", "type": "STRING"},
            {"name": "estado", "type": "STRING"},
            {"name": "estado_civil", "type": "STRING"},
            {"name": "num_poliza", "type": "STRING"},
            {"name": "tipo_moneda", "type": "STRING"}
        ]
    },
    
    # Titular
    {
        "name": "titular", 
        "type": "RECORD",
        "fields": [
            {"name": "nombre_completo", "type": "STRING"},
            {"name": "tipo_documento", "type": "STRING"},
            {"name": "num_documento", "type": "STRING"},
            {"name": "tipo_afiliacion", "type": "STRING"},
            {"name": "numero_plan", "type": "STRING"},
            {"name": "plan_salud", "type": "STRING"},
            {"name": "contratante", "type": "STRING"}
        ]
    },
    
    # Metadata
    {
        "name": "metadata", 
        "type": "RECORD",
        "fields": [
            {"name": "fecha_hora_autorizacion", "type": "STRING"},
            {"name": "fecha_hora_impresion", "type": "STRING"}
        ]
    },
    
    # Beneficio Autorizado
    {
        "name": "beneficio_autorizado", 
        "type": "RECORD",
        "fields": [
            {"name": "codigo", "type": "STRING"},
            {"name": "nombre", "type": "STRING"},
            {"name": "restricciones", "type": "STRING"},
            {"name": "copago_fijo", "type": "STRING"},
            {"name": "copago_variable", "type": "STRING"},
            {"name": "fin_carencia", "type": "STRING"},
            {"name": "observacion", "type": "STRING"}
        ]
    },
    
    # Procedimiento Copago
    {
        "name": "procedimiento_copago", 
        "type": "RECORD",
        "fields": [
            {"name": "codigo", "type": "STRING"},
            {"name": "procedimiento", "type": "STRING"},
            {"name": "sexo", "type": "STRING"},
            {"name": "copago_fijo", "type": "NUMERIC"},
            {"name": "copago_variable", "type": "STRING"},
            {"name": "frecuencia", "type": "NUMERICEGER"},
            {"name": "tiempo_dias", "type": "STRING"},
            {"name": "observaciones", "type": "STRING"},
            {"name": "observaciones_asegurado", "type": "STRING"},
            {"name": "observaciones_adicionales", "type": "STRING"}
        ]
    },
    
    # Datos Clínicos
    {
        "name": "datos_clinicos", 
        "type": "RECORD",
        "fields": [
            {"name": "sintomas_signos", "type": "STRING"},
            {"name": "tiempo_enfermedad", "type": "STRING"},
            {"name": "antecedentes", "type": "STRING"},
            {"name": "num_consultas", "type": "STRING"},
            {"name": "fechas_consultas", "type": "STRING"}
        ]
    },
    
    # Diagnosticos (ARRAY de RECORD)
    {
        "name": "diagnosticos",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "codigo", "type": "STRING"},
            {"name": "descripcion", "type": "STRING"}
        ]
    },
    
    # Visación Médico
    {
        "name": "visacion_medico", 
        "type": "RECORD",
        "fields": [
            {"name": "nombre", "type": "STRING"},
            {"name": "codigo_cmp", "type": "STRING"}
        ]
    },
    
]

CREDIT_NOTE_SCHEMA =  [
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    
    {
        "name": "encabezado",
        "type": "RECORD",
        "fields": [
        {"name": "empresa", "type": "STRING"},
        {"name": "direccion", "type": "STRING"}
        ]
    },
    {
        "name": "recuadro",
        "type": "RECORD",
        "fields": [
        {"name": "ruc", "type": "STRING"},
        {"name": "titulo", "type": "STRING"},
        {"name": "codigo", "type": "STRING"},
        {"name": "codigo_2", "type": "STRING"},
        {"name": "fecha", "type": "STRING"}
        ]
    },
    {
        "name": "info_principal",
        "type": "RECORD",
        "fields": [
        {"name": "nomb_receptor", "type": "STRING"},
        {"name": "ruc_receptor", "type": "STRING"},
        {"name": "dir_receptor", "type": "STRING"},
        {"name": "nomb_paciente", "type": "STRING"},
        {"name": "dni_paciente", "type": "STRING"},
        {"name": "carta_paciente", "type": "STRING"},
        {"name": "ref_tipo_doc", "type": "STRING"},
        {"name": "ref_nro", "type": "STRING"},
        {"name": "cond_pago", "type": "STRING"},
        {"name": "encuentro", "type": "STRING"}
        ]
    },
    {
        "name": "resumen",
        "type": "RECORD",
        "fields": [
        {"name": "importe_total_str", "type": "STRING"},
        {"name": "subtotal", "type": "NUMERIC"},
        {"name": "op_gravada", "type": "NUMERIC"},
        {"name": "op_inafecta", "type": "NUMERIC"},
        {"name": "op_exonerada", "type": "NUMERIC"},
        {"name": "op_gratuita", "type": "NUMERIC"},
        {"name": "igv", "type": "NUMERIC"},
        {"name": "icpber", "type": "NUMERIC"},
        {"name": "total_venta", "type": "NUMERIC"},
        {"name": "descuento", "type": "NUMERIC"},
        {"name": "importe_total", "type": "NUMERIC"}
        ]
    }
]

PRE_SETTLEMENT_SCHEMA = [
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    {
        "name": "cabecera",
        "type": "RECORD",
        "fields": [
            {"name": "titulo", "type": "STRING"},
            {"name": "sede", "type": "STRING"},
            {"name": "garante", "type": "STRING"},
            {"name": "empresa", "type": "STRING"},
            {"name": "deducible", "type": "NUMERIC"},
            {"name": "coaseguro", "type": "NUMERIC"},
            {"name": "num_autoriz", "type": "STRING"},
            {"name": "paciente", "type": "STRING"},
            {"name": "titular", "type": "STRING"},
            {"name": "tratante", "type": "STRING"},
            {"name": "hist_clin", "type": "STRING"},
            {"name": "num_cama", "type": "STRING"},
            {"name": "fec_ingreso", "type": "STRING"},
            {"name": "fec_alta", "type": "STRING"},
            {"name": "num_encuentro", "type": "STRING"},
            {"name": "beneficio", "type": "STRING"},
        ],
    },
    {
        "name": "subgrupos",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "categoria", "type": "STRING"},
            {
                "name": "items",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "codigo", "type": "STRING"},
                    {"name": "descripcion", "type": "STRING"},
                    {"name": "fec_registro", "type": "STRING"},
                    {"name": "cantidad", "type": "NUMERIC"},
                    {"name": "precio_unitario", "type": "NUMERIC"},
                    {"name": "importe", "type": "NUMERIC"},
                    {"name": "igv", "type": "NUMERIC"},
                    {"name": "paciente", "type": "NUMERIC"},
                    {"name": "facturar", "type": "NUMERIC"},
                ],
            },
            {
                "name": "subtotal",
                "type": "RECORD",
                "fields": [
                    {"name": "precio_unitario", "type": "NUMERIC"},
                    {"name": "importe", "type": "NUMERIC"},
                    {"name": "paciente", "type": "NUMERIC"},
                    {"name": "facturar", "type": "NUMERIC"},
                ],
            },
        ],
    },
    {
        "name": "gastos_afectos",
        "type": "RECORD",
        "fields": [
            {"name": "total", "type": "NUMERIC"},
            {"name": "coaseguro_paciente", "type": "NUMERIC"},
            {"name": "garante", "type": "NUMERIC"},
            {"name": "monto_afecto", "type": "NUMERIC"},
            {"name": "igv", "type": "NUMERIC"},
            {"name": "total_facturar", "type": "NUMERIC"},
        ],
    },
    {
        "name": "gastos_inafectos",
        "type": "RECORD",
        "fields": [
            {"name": "total", "type": "NUMERIC"},
            {"name": "coaseguro_paciente", "type": "NUMERIC"},
            {"name": "garante", "type": "NUMERIC"},
        ],
    }
]

SETTLEMENT_SUMMARY_SCHEMA = [
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    {
        "name": "cabecera",
        "type": "RECORD",
        "fields": [
            {"name": "titulo", "type": "STRING"},
            {"name": "sede", "type": "STRING"},
            {"name": "garante", "type": "STRING"},
            {"name": "empresa", "type": "STRING"},
            {"name": "deducible", "type": "NUMERIC"},
            {"name": "coaseguro", "type": "NUMERIC"},
            {"name": "num_autoriz", "type": "STRING"},
            {"name": "paciente", "type": "STRING"},
            {"name": "titular", "type": "STRING"},
            {"name": "tratante", "type": "STRING"},
            {"name": "hist_clin", "type": "STRING"},
            {"name": "num_cama", "type": "STRING"},
            {"name": "fec_ingreso", "type": "STRING"},
            {"name": "fec_alta", "type": "STRING"},
            {"name": "num_encuentro", "type": "STRING"},
            {"name": "beneficio", "type": "STRING"},
            {"name": "mecanismo", "type": "STRING"},
            {"name": "monto_cpm", "type": "NUMERIC"},
            {"name": "tipo_moneda", "type": "STRING"},
        ],
    },
    {
        "name": "calculo_cpm",
        "type": "RECORD",
        "fields": [
            {"name": "cpm", "type": "NUMERIC"},
            {"name": "cpm_igv", "type": "NUMERIC"},
            {"name": "total_deducible", "type": "NUMERIC"},
            {"name": "total_coaseguros", "type": "NUMERIC"},
            {"name": "total_facturar", "type": "NUMERIC"},
        ],
    }
]

SETTLEMENT_TYPE_1_SCHEMA = [
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    {
        "name": "cabecera",
        "type": "RECORD",
        "fields": [
            {"name": "titulo", "type": "STRING"},
            {"name": "sede", "type": "STRING"},
            {"name": "garante", "type": "STRING"},
            {"name": "empresa", "type": "STRING"},
            {"name": "deducible", "type": "NUMERIC"},
            {"name": "coaseguro", "type": "NUMERIC"},
            {"name": "num_autoriz", "type": "STRING"},
            {"name": "paciente", "type": "STRING"},
            {"name": "titular", "type": "STRING"},
            {"name": "tratante", "type": "STRING"},
            {"name": "hist_clin", "type": "STRING"},
            {"name": "fec_ingreso", "type": "STRING"},
            {"name": "fec_alta", "type": "STRING"},
            {"name": "num_encuentro", "type": "STRING"},
            {"name": "beneficio", "type": "STRING"},
            {"name": "mecanismo", "type": "STRING"},
            {"name": "monto_cpm", "type": "NUMERIC"},
            {"name": "tipo_moneda", "type": "STRING"},
        ],
    },
    {
        "name": "subgrupos",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "categoria", "type": "STRING"},
            {
                "name": "items",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "codigo", "type": "STRING"},
                    {"name": "descripcion", "type": "STRING"},
                    {"name": "fec_registro", "type": "STRING"},
                    {"name": "cantidad", "type": "NUMERIC"},
                    {"name": "precio_unitario", "type": "NUMERIC"},
                    {"name": "importe", "type": "NUMERIC"},
                    {"name": "igv", "type": "NUMERIC"},
                    {"name": "coaseguro", "type": "NUMERIC"},
                    {"name": "paciente", "type": "NUMERIC"},
                    {"name": "importe_garante", "type": "NUMERIC"},
                ],
            },
            {
                "name": "subtotal",
                "type": "RECORD",
                "fields": [
                    {"name": "precio_unitario", "type": "NUMERIC"},
                    {"name": "importe", "type": "NUMERIC"},
                    {"name": "paciente", "type": "NUMERIC"},
                    {"name": "importe_garante", "type": "NUMERIC"},
                ],
            },
        ],
    },
    {
        "name": "gastos_afectos",
        "type": "RECORD",
        "fields": [
            {"name": "total_garante", "type": "NUMERIC"},
            {"name": "deducible_igv", "type": "NUMERIC"},
            {"name": "coaseguro_igv", "type": "NUMERIC"},
        ],
    },
    {
        "name": "gastos_inafectos",
        "type": "RECORD",
        "fields": [
            {"name": "total_garante", "type": "NUMERIC"},
            {"name": "coaseguro", "type": "NUMERIC"},
        ],
    },
]

SETTLEMENT_TYPE_2_SCHEMA = [
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    {
        "name": "cabecera",
        "type": "RECORD",
        "fields": [
            {"name": "titulo", "type": "STRING"},
            {"name": "garante", "type": "STRING"},
            {"name": "num_factura", "type": "STRING"},
            {"name": "empresa", "type": "STRING"},
            {"name": "deducible", "type": "STRING"},
            {"name": "num_autoriz", "type": "STRING"},
            {"name": "paciente", "type": "STRING"},
            {"name": "titular", "type": "STRING"},
            {"name": "tratante", "type": "STRING"},
            {"name": "beneficio", "type": "STRING"},
            {"name": "hist_clin", "type": "STRING"},
            {"name": "fec_ingreso", "type": "STRING"},
            {"name": "coaseguro", "type": "STRING"},
            {"name": "num_encuentro", "type": "STRING"},
            {"name": "fec_alta", "type": "STRING"},
            {"name": "mecanismo", "type": "STRING"},
            {"name": "num_documento", "type": "STRING"},
        ],
    },
    {
        "name": "subgrupos",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "categoria", "type": "STRING"},
            {
                "name": "items",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "codigo", "type": "STRING"},
                    {"name": "descripcion", "type": "STRING"},
                    {"name": "fec_registro", "type": "STRING"},
                    {"name": "cantidad", "type": "NUMERIC"},
                    {"name": "precio_unitario", "type": "NUMERIC"},
                    {"name": "importe", "type": "NUMERIC"},
                    {"name": "igv", "type": "NUMERIC"},
                    {"name": "coaseguro", "type": "NUMERIC"},
                    {"name": "paciente", "type": "NUMERIC"},
                    {"name": "total_inafectos", "type": "NUMERIC"},
                    {"name": "total_afectos", "type": "NUMERIC"},
                ],
            },
            {
                "name": "subtotal",
                "type": "RECORD",
                "fields": [
                    {"name": "precio_unitario", "type": "NUMERIC"},
                    {"name": "importe", "type": "NUMERIC"},
                    {"name": "paciente", "type": "NUMERIC"},
                    {"name": "total_inafectos", "type": "NUMERIC"},
                    {"name": "total_afectos", "type": "NUMERIC"},
                ],
            },
        ],
    },
    {
        "name": "gastos_afectos",
        "type": "RECORD",
        "fields": [
            {"name": "subtotal_1", "type": "NUMERIC"},
            {"name": "deducible_igv", "type": "NUMERIC"},
            {"name": "subtotal_2", "type": "NUMERIC"},
            {"name": "coaseguro_igv", "type": "NUMERIC"},
            {"name": "subtotal_3", "type": "NUMERIC"},
            {"name": "igv", "type": "NUMERIC"},
            {"name": "total_facturar", "type": "NUMERIC"},
        ],
    },
    {
        "name": "gastos_inafectos",
        "type": "RECORD",
        "fields": [
            {"name": "subtotal_1", "type": "NUMERIC"},
            {"name": "coaseguro", "type": "NUMERIC"},
            {"name": "total_garante", "type": "NUMERIC"},
        ],
    },
]

GUARANTEE_LETTER_SCHEMA = [
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    {
        "name": "encabezado",
        "type": "RECORD",
        "fields": [
            {"name": "titulo", "type": "STRING"},
            {"name": "proveedor_ruc", "type": "STRING"},
            {"name": "proveedor_nombre", "type": "STRING"},
            {"name": "usuario", "type": "STRING"},
            {"name": "compania", "type": "STRING"},
            {"name": "nro_carta", "type": "STRING"},
            {"name": "fec_emision", "type": "STRING"},
            {"name": "fec_val_sol", "type": "STRING"}
        ]
    },
    {
        "name": "info_contratante",
        "type": "RECORD",
        "fields": [
            {"name": "nro_contrato", "type": "STRING"},
            {"name": "fec_inic_vig", "type": "STRING"},
            {"name": "razon_social", "type": "STRING"},
            {"name": "titular", "type": "STRING"}
        ]
    },
    {
        "name": "info_paciente",
        "type": "RECORD",
        "fields": [
            {"name": "ape_paterno", "type": "STRING"},
            {"name": "ape_materno", "type": "STRING"},
            {"name": "nombres", "type": "STRING"},
            {"name": "nro_carnet", "type": "STRING"},
            {"name": "cod_afiliado", "type": "STRING"},
            {"name": "fec_nacimiento", "type": "STRING"},
            {"name": "edad", "type": "NUMERIC"},
            {"name": "sexo", "type": "STRING"},
            {"name": "parentesco", "type": "STRING"},
            {"name": "obs_asegurado", "type": "STRING"}
        ]
    },
    {
        "name": "informe_medico",
        "type": "RECORD",
        "fields": [
            {"name": "proc_medico", "type": "STRING"},
            {"name": "diagnostico", "type": "STRING"}
        ]
    },
    {
        "name": "limites_garantizados",
        "type": "RECORD",
        "fields": [
            {"name": "cobertura", "type": "STRING"},
            {"name": "deducible", "type": "STRING"},
            {"name": "cubierto", "type": "NUMERIC"},
            {"name": "monto_total", "type": "STRING"},
            {"name": "monto_acumulado", "type": "STRING"},
            {"name": "nota", "type": "STRING"}
        ]
    },
    {
        "name": "observaciones",
        "type": "RECORD",
        "fields": [
            {"name": "observaciones", "type": "STRING"},
            {"name": "rimac_empresa", "type": "STRING"},
            {"name": "rimac_ruc", "type": "STRING"}
        ]
    },
    {
        "name": "firma",
        "type": "RECORD",
        "fields": [
            {"name": "lugar_fecha", "type": "STRING"},
            {"name": "auditor_admin", "type": "STRING"}
        ]
    }
]

EPICRISIS_SCHEMA = [
    {
        'name': 'documento_id', 'type': 'STRING'
    },
    {
        'name': 'page_path', 'type': 'STRING'
    },
    {
        'name': 'encabezado',
        'type': 'RECORD',
        'fields': [
            {'name': 'titulo', 'type': 'STRING'},
            {'name': 'direccion', 'type': 'STRING'},
            {
                'name': 'encuentros', 'type': 'STRING',
                'mode': 'REPEATED'
            }
        ]
    },
    {
        'name': 'info_ingreso', 
        'type': 'RECORD', 
        'fields': [
            {'name': 'paciente', 'type': 'STRING'},
            {'name': 'fec_ingreso', 'type': 'STRING'},
            {'name': 'servicio', 'type': 'STRING'},
            {'name': 'hora_ingreso', 'type': 'STRING'},
            {'name': 'cama', 'type': 'STRING'},
            {'name': 'nhc', 'type': 'STRING'},
            {'name': 'resumen', 'type': 'STRING'},
            {'name': 'complicaciones', 'type': 'STRING'},
            {
                'name': 'diagnosticos_ingreso', 
                'type': 'RECORD', 
                'mode': 'REPEATED', 
                'fields': [
                    {'name': 'cie10', 'type': 'STRING'},
                    {'name': 'detalle', 'type': 'STRING'}
                ]},
            {
                'name': 'proc_realizados', 
                'type': 'STRING', 
                'mode': 'REPEATED'
            }
    ]},
    {
        'name': 'info_egreso', 'type': 'RECORD', 'fields': [
        {'name': 'fec_egreso', 'type': 'STRING'},
        {'name': 'hora_egreso', 'type': 'STRING'},
        {'name': 'estadia', 'type': 'NUMERIC'},
        {'name': 'tipo_alta', 'type': 'STRING'},
        {'name': 'condicion_alta', 'type': 'STRING'},
        {'name': 'pronostico', 'type': 'STRING'},
        {'name': 'medico_tratante', 'type': 'STRING'},
        {'name': 'diagnosticos_salida', 'type': 'RECORD', 'fields': [
            {'name': 'diagnostico_principal', 'type': 'STRING'},
            {'name': 'diagnosticos_salida', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
                {'name': 'tipo', 'type': 'STRING'},
                {'name': 'cie10', 'type': 'STRING'},
                {'name': 'detalle', 'type': 'STRING'},
                {'name': 'etapa', 'type': 'STRING'}
            ]}
        ]}
    ]},
    {
        'name': 'firma', 
        'type': 'RECORD', 
        'fields': [
        {'name': 'nomb_doctor', 'type': 'STRING'},
        {'name': 'lugar', 'type': 'STRING'},
        {'name': 'fecha', 'type': 'STRING'},
        {'name': 'num_colegiado', 'type': 'STRING'}
    ]}
]

PRESCRIPTION_SCHEMA = [
    {'name': 'documento_id', 'type': 'STRING'},
    {'name': 'page_path', 'type': 'STRING'},
    {'name': 'encabezado', 'type': 'RECORD', 'fields': [
        {'name': 'titulo', 'type': 'STRING'},
        {'name': 'direccion', 'type': 'STRING'},
        {'name': 'fecha', 'type': 'STRING'},
        {'name': 'hora', 'type': 'STRING'}
    ]},
    {'name': 'info_principal', 'type': 'RECORD', 'fields': [
        {'name': 'episodio', 'type': 'STRING'},
        {'name': 'nhc', 'type': 'STRING'},
        {'name': 'poliza', 'type': 'STRING'},
        {'name': 'edad', 'type': 'STRING'},
        {'name': 'aseguradora', 'type': 'STRING'},
        {'name': 'empleadora', 'type': 'STRING'},
        {'name': 'deducible', 'type': 'NUMERIC'},
        {'name': 'especialidad', 'type': 'STRING'},
        {'name': 'rne', 'type': 'STRING'},
        {'name': 'medico', 'type': 'STRING'},
        {'name': 'preexistencias', 'type': 'STRING'},
        {'name': 'diagnosticos', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
            {'name': 'tipo', 'type': 'STRING'},
            {'name': 'cie10', 'type': 'STRING'},
            {'name': 'detalle', 'type': 'STRING'},
            {'name': 'etapa', 'type': 'STRING'}
        ]}
    ]},
    {'name': 'medicamentos', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
        {'name': 'fec_inicio', 'type': 'STRING'},
        {'name': 'nomb_generico', 'type': 'STRING'},
        {'name': 'dosis', 'type': 'STRING'},
        {'name': 'frec', 'type': 'STRING'},
        {'name': 'dias', 'type': 'NUMERIC'},
        {'name': 'cant_total', 'type': 'NUMERIC'},
        {'name': 'via', 'type': 'STRING'},
        {'name': 'indicaciones', 'type': 'STRING'}
    ]},
    {'name': 'firma', 'type': 'RECORD', 'fields': [
        {'name': 'nomb_doctor', 'type': 'STRING'},
        {'name': 'lugar', 'type': 'STRING'},
        {'name': 'fecha', 'type': 'STRING'},
        {'name': 'num_colegiado', 'type': 'STRING'}
    ]}
]

PHARMACY_ATTENTION_SCHEMA = [
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    {
        "name": "encabezado",
        "type": "RECORD",
        "fields": [
            {"name": "titulo", "type": "STRING"},
            {"name": "direccion", "type": "STRING"},
            {"name": "fecha", "type": "STRING"},
            {"name": "hora", "type": "STRING"},
        ],
    },
    {
        "name": "info_principal",
        "type": "RECORD",
        "fields": [
            {"name": "usuario", "type": "STRING"},
            {"name": "num_guia", "type": "STRING"},
            {"name": "episodio", "type": "STRING"},
            {"name": "nhc", "type": "STRING"},
            {"name": "poliza", "type": "STRING"},
            {"name": "dni", "type": "STRING"},
            {"name": "aseguradora", "type": "STRING"},
            {"name": "empleadora", "type": "STRING"},
            {"name": "deducible", "type": "STRING"},
            {"name": "especialidad", "type": "STRING"},
            {"name": "rne", "type": "STRING"},
            {"name": "medico", "type": "STRING"},
            {"name": "coaseguro", "type": "STRING"},
            {"name": "cie10", "type": "STRING"},
        ],
    },
    {
        "name": "medicamentos",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "dci_generico", "type": "STRING"},
            {"name": "medicamento", "type": "STRING"},
            {"name": "u_m", "type": "STRING"},
            {"name": "dosis", "type": "STRING"},
            {"name": "cantidad", "type": "FLOAT"},
            {"name": "via", "type": "STRING"},
            {"name": "estado", "type": "STRING"},
        ],
    },
    {
        "name": "firma",
        "type": "RECORD",
        "fields": [
            {"name": "dni", "type": "STRING"},
            {"name": "telf", "type": "STRING"},
        ],
    },
]

OPERATORY_REPORT_SCHEMA = [
    {"name": "documento_id", "type": "STRING"},
    {"name": "page_path", "type": "STRING"},
    {
        "name": "encabezado",
        "type": "RECORD",
        "fields": [
            {"name": "titulo", "type": "STRING"},
            {"name": "direccion", "type": "STRING"},
            {
                "name": "encuentros",
                "type": "STRING",
                "mode": "REPEATED"
            }
        ],
    },
    {
        "name": "info_principal",
        "type": "RECORD",
        "fields": [
            {"name": "paciente", "type": "STRING"},
            {"name": "nhc", "type": "STRING"},
            {"name": "edad", "type": "FLOAT"},
            {"name": "sexo", "type": "STRING"},
            {"name": "servicio", "type": "STRING"},
            {"name": "cama", "type": "STRING"},
        ],
    },
    {
        "name": "info_proceso",
        "type": "RECORD",
        "fields": [
            {
                "name": "diagnostico_pre",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "cie10", "type": "STRING"},
                    {"name": "detalle", "type": "STRING"},
                ],
            },
            {
                "name": "diagnostico_post",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "cie10", "type": "STRING"},
                    {"name": "detalle", "type": "STRING"},
                ],
            },
            {"name": "cirugias_programadas", "type": "STRING", "mode": "REPEATED"},
            {"name": "cirugias_realizadas", "type": "STRING", "mode": "REPEATED"},
            {"name": "medico_responsable", "type": "STRING"},
            {"name": "anestesiologo", "type": "STRING"},
            {"name": "primer_ayudante", "type": "STRING"},
            {"name": "segundo_ayudante", "type": "STRING"},
            {"name": "tercer_ayudante", "type": "STRING"},
            {"name": "instrumentista", "type": "STRING"},
            {"name": "circulante", "type": "STRING"},
            {"name": "perfusionista", "type": "STRING"},
            {"name": "neonatologo", "type": "STRING"},
            {"name": "tipo_anestesia", "type": "STRING"},
            {"name": "hora_inicio", "type": "STRING"},
            {"name": "hora_termino", "type": "STRING"},
            {"name": "lateralidad", "type": "STRING"},
            {"name": "hallaz_operat", "type": "STRING"},
            {"name": "descrip_proced", "type": "STRING"},
            {"name": "complicaciones", "type": "STRING"},
        ],
    },
    {
        "name": "info_detalle",
        "type": "RECORD",
        "fields": [
            {"name": "vol_sangrado", "type": "FLOAT"},
            {"name": "solicitud_ap_1", "type": "STRING"},
            {"name": "solicitud_ap_2", "type": "STRING"},
            {"name": "ctj_gas_compl", "type": "STRING"},
            {"name": "congelacion_1", "type": "STRING"},
            {"name": "congelacion_2", "type": "STRING"},
            {"name": "ctj_inst_compl", "type": "STRING"},
            {"name": "cultivo_1", "type": "STRING"},
            {"name": "cultivo_2", "type": "STRING"},
            {"name": "estado_hemo", "type": "STRING"},
            {"name": "info_familiar", "type": "STRING"},
            {"name": "destino_paciente", "type": "STRING"},
        ],
    },
    {
        "name": "firma",
        "type": "RECORD",
        "fields": [
            {"name": "nomb_doctor", "type": "STRING"},
            {"name": "lugar", "type": "STRING"},
            {"name": "fecha", "type": "STRING"},
            {"name": "num_colegiado", "type": "STRING"},
        ],
    },
]



MAPPER_AUNA_DOCS = {
    DocumentKeyEnum.INVOICE_IMPORT_KEY.value: INVOICE_SCHEMA,
    DocumentKeyEnum.SITED_IMPORT_KEY.value: SITED_SCHEMA,
    DocumentKeyEnum.CREDIT_NOTE_KEY.value: CREDIT_NOTE_SCHEMA,

    DocumentKeyEnum.PRE_SETTLEMENT_KEY.value: PRE_SETTLEMENT_SCHEMA,
    DocumentKeyEnum.SETTLEMENT_SUMMARY_KEY.value: SETTLEMENT_SUMMARY_SCHEMA,
    DocumentKeyEnum.SETTLEMENT_TYPE_1_KEY.value: SETTLEMENT_TYPE_1_SCHEMA,
    DocumentKeyEnum.SETTLEMENT_TYPE_2_KEY.value: SETTLEMENT_TYPE_2_SCHEMA,

    DocumentKeyEnum.GUARANTEE_LETTER_KEY.value: GUARANTEE_LETTER_SCHEMA,
    DocumentKeyEnum.EPICRISIS_KEY.value: EPICRISIS_SCHEMA,
    DocumentKeyEnum.PRESCRIPTION_KEY.value: PRESCRIPTION_SCHEMA,
    DocumentKeyEnum.PHARMACY_ATTENTION_KEY.value: PHARMACY_ATTENTION_SCHEMA,
    DocumentKeyEnum.OPERATORY_REPORT_KEY.value: OPERATORY_REPORT_SCHEMA,

}