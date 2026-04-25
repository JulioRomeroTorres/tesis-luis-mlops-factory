DEFAULT_ATTACHMENT_TEMPLATE = """
Hola equipo, <br>
<br>
Les compartimos el status de las facturas ingresadas el día {current_date} de AUNA:<br>
<br>

<ul style="list-style-type: none; padding-left: 0;">
  <li><strong> Total facturas procesadas: </strong>{invoice_number} facturas </li>
  <li><strong> Monto facturado:</strong> S/ {invoice_amount} </li>
  <li><strong> Facturas observadas por la fábrica: </strong> {invoice_number_obs} <strong> ({invoice_number_obs_percentage} % del total de factura) </strong> </li>
  <li><strong> Monto observado: </strong> S/ {obs_amount}  </li>
  <li><strong> Monto con potencial de ahorro:</strong> S/ {potencial_amount}  <strong> ( {potencial_amount_percentage} % del monto observado) </strong> </li>
</ul>

<br>
Adicionalmente, adjuntamos archivo con el detalle de las facturas y alertas generadas para su revisión por auditoría <br>
Hacerle click en el link de abajo<b><br><br>
<a href='{signed_url}' download='archivo_desde_url'>Alertas_IA_AUNA_{current_date}</a>
<br>
<strong>Saludos,<strong><br>
<br>
<strong>Equipo Advanced Analytics<strong>
"""

DEFAULT_ATTACHMENT_WITH_TOTAL_REJECTIONS_INFORMATION_TEMPLATE = """
Hola equipo, <br>
<br>
Les compartimos el status de las facturas ingresadas el día {current_date} de AUNA:<br>
<br>

<ul style="list-style-type: none; padding-left: 0;">
  <li><strong> Total facturas procesadas: </strong>{invoice_number} facturas </li>
  <li><strong> Monto facturado:</strong> S/ {invoice_amount} </li>
  <li><strong> Facturas observadas por la fábrica: </strong> {invoice_number_obs} <strong> ({invoice_number_obs_percentage} % del total de factura) </strong> </li>
  <li><strong> Monto observado: </strong> S/ {obs_amount}  </li>
</ul>
<br>
Adicionalmente, les compartimos las facturas con alertas de rechazo total de los 6 grupos de clínica ingresadas el día {current_date}:
<br>
<ul style="list-style-type: none; padding-left: 0;">
  <li><strong> Facturas con rechazo total: </strong>{total_rejected_invoice} facturas </li>
  <li><strong> Monto de rechazo total:</strong> S/ {total_rejected_amount} </li>
</ul>
<br>

<br>
Adicionalmente, adjuntamos archivo con el detalle de las facturas y alertas generadas para su revisión por auditoría <br>
Hacerle click en el link de abajo<b><br><br>
<a href='{signed_url}' download='archivo_desde_url'>Alertas_IA_AUNA_{current_date}</a>
<br>
<strong>Saludos,<strong><br>
<br>
<strong>Equipo Advanced Analytics<strong>
"""

AUNA_PROCESS_SUMMARY_SUBJECT = "Alertas Fabrica Siniestros SALUD - AUNA - Flujo regular"