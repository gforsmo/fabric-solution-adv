CREATE TABLE [metadata].[log_store] (
    [log_id]          INT            NOT NULL,
    [function_name]   VARCHAR (100)  NOT NULL,
    [description]     VARCHAR (1000) NULL,
    [expected_params] JSON           NULL,
    PRIMARY KEY CLUSTERED ([log_id] ASC)
);


GO


-- Trigger for metadata.log_store
CREATE   TRIGGER [metadata].trg_log_store_audit
ON metadata.log_store
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.log_store', i.log_id, 'insert', NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.log_id = i.log_id);

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.log_store', i.log_id, 'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.log_id = d.log_id;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.log_store', d.log_id, 'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), NULL
    FROM deleted d
    WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i.log_id = d.log_id);
END;

GO

