CREATE TABLE [metadata].[expectation_store] (
    [expectation_id]   INT            NOT NULL,
    [expectation_name] VARCHAR (100)  NOT NULL,
    [gx_method]        VARCHAR (100)  NOT NULL,
    [description]      VARCHAR (1000) NULL,
    [expected_params]  JSON           NULL,
    PRIMARY KEY CLUSTERED ([expectation_id] ASC)
);


GO


-- Trigger for metadata.expectation_store
CREATE   TRIGGER [metadata].trg_expectation_store_audit
ON metadata.expectation_store
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.expectation_store', i.expectation_id, 'insert', NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.expectation_id = i.expectation_id);

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.expectation_store', i.expectation_id, 'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.expectation_id = d.expectation_id;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.expectation_store', d.expectation_id, 'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), NULL
    FROM deleted d
    WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i.expectation_id = d.expectation_id);
END;

GO

