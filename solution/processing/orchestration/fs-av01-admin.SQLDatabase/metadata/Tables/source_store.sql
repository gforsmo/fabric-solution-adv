CREATE TABLE [metadata].[source_store] (
    [source_id]     INT            NOT NULL,
    [source_name]   VARCHAR (100)  NOT NULL,
    [source_type]   VARCHAR (50)   NOT NULL,
    [auth_method]   VARCHAR (50)   NULL,
    [key_vault_url] VARCHAR (500)  NULL,
    [secret_name]   VARCHAR (100)  NULL,
    [base_url]      VARCHAR (500)  NULL,
    [description]   VARCHAR (1000) NULL,
    [created_date]  DATETIME2 (7)  DEFAULT (getdate()) NULL,
    [modified_date] DATETIME2 (7)  DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([source_id] ASC)
);


GO


-- Trigger for metadata.source_store
CREATE   TRIGGER [metadata].trg_source_store_audit
ON metadata.source_store
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Handle INSERTs
    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT
        'metadata.source_store',
        i.source_id,
        'insert',
        NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.source_id = i.source_id);

    -- Handle UPDATEs
    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT
        'metadata.source_store',
        i.source_id,
        'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.source_id = d.source_id;

    -- Handle DELETEs
    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT
        'metadata.source_store',
        d.source_id,
        'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        NULL
    FROM deleted d
    WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i.source_id = d.source_id);
END;

GO

