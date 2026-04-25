INSERT INTO `{{project_id}}.genai_documents.auna_{{entity}}_mvp` (
    SELECT 
    original_table.id as documento_id,
    tmp_entities.page_path,
    tmp_entities.*EXCEPT(documento_id, page_path),
    CURRENT_TIMESTAMP() as created_at
    FROM  `{{project_id}}.genai_documents.auna_documents` as original_table
    INNER JOIN `{{project_id}}.tmp.auna_documents` as tmp_table
    on original_table.file_path = tmp_table.file_path
    INNER JOIN `{{project_id}}.tmp.auna_{{entity}}_mvp` as tmp_entities
    on tmp_table.id = tmp_entities.documento_id
);
