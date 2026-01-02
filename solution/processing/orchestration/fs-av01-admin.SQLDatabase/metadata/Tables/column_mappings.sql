CREATE TABLE [metadata].[column_mappings] (
    [mapping_id]    VARCHAR (100) NOT NULL,
    [column_order]  INT           NOT NULL,
    [source_column] VARCHAR (255) NOT NULL,
    [target_column] VARCHAR (100) NOT NULL,
    [data_type]     VARCHAR (50)  NOT NULL,
    [description]   VARCHAR (500) NULL,
    PRIMARY KEY CLUSTERED ([mapping_id] ASC, [column_order] ASC)
);


GO

CREATE NONCLUSTERED INDEX [IX_column_mappings_id]
    ON [metadata].[column_mappings]([mapping_id] ASC);


GO


-- Trigger for metadata.column_mappings
CREATE   TRIGGER [metadata].trg_column_mappings_audit
ON metadata.column_mappings
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Handle INSERTs
    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT
        'metadata.column_mappings',
        0,  -- No single record_id for composite key
        'insert',
        NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (
        SELECT 1 FROM deleted d
        WHERE d.mapping_id = i.mapping_id AND d.column_order = i.column_order
    );

    -- Handle UPDATEs
    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT
        'metadata.column_mappings',
        0,
        'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.mapping_id = d.mapping_id AND i.column_order = d.column_order;

    -- Handle DELETEs
    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT
        'metadata.column_mappings',
        0,
        'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        NULL
    FROM deleted d
    WHERE NOT EXISTS (
        SELECT 1 FROM inserted i
        WHERE i.mapping_id = d.mapping_id AND i.column_order = d.column_order
    );
END;

GO

