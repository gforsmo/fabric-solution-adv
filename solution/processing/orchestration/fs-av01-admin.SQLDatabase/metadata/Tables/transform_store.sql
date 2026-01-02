CREATE TABLE [metadata].[transform_store] (
    [transform_id]    INT            NOT NULL,
    [function_name]   VARCHAR (100)  NOT NULL,
    [description]     VARCHAR (1000) NULL,
    [expected_params] JSON           NULL,
    PRIMARY KEY CLUSTERED ([transform_id] ASC)
);


GO


-- Trigger for metadata.transform_store
CREATE   TRIGGER [metadata].trg_transform_store_audit
ON metadata.transform_store
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.transform_store', i.transform_id, 'insert', NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.transform_id = i.transform_id);

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.transform_store', i.transform_id, 'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.transform_id = d.transform_id;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.transform_store', d.transform_id, 'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), NULL
    FROM deleted d
    WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i.transform_id = d.transform_id);
END;

GO

