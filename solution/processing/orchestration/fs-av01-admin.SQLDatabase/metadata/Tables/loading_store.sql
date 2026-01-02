CREATE TABLE [metadata].[loading_store] (
    [loading_id]      INT            NOT NULL,
    [function_name]   VARCHAR (100)  NOT NULL,
    [description]     VARCHAR (1000) NULL,
    [expected_params] JSON           NULL,
    PRIMARY KEY CLUSTERED ([loading_id] ASC)
);


GO


-- Trigger for metadata.loading_store
CREATE   TRIGGER [metadata].trg_loading_store_audit
ON metadata.loading_store
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.loading_store', i.loading_id, 'insert', NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.loading_id = i.loading_id);

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.loading_store', i.loading_id, 'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.loading_id = d.loading_id;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.loading_store', d.loading_id, 'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), NULL
    FROM deleted d
    WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i.loading_id = d.loading_id);
END;

GO

