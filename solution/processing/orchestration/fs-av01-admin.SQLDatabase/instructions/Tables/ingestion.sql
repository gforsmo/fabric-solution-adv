CREATE TABLE [instructions].[ingestion] (
    [ingestion_id]    INT           NOT NULL,
    [source_id]       INT           NOT NULL,
    [endpoint_path]   VARCHAR (500) NULL,
    [landing_path]    VARCHAR (500) NOT NULL,
    [request_params]  JSON          NULL,
    [is_active]       BIT           DEFAULT ((1)) NULL,
    [log_function_id] INT           NULL,
    [created_date]    DATETIME2 (7) DEFAULT (getdate()) NULL,
    [modified_date]   DATETIME2 (7) DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([ingestion_id] ASC),
    CONSTRAINT [FK_ingestion_log] FOREIGN KEY ([log_function_id]) REFERENCES [metadata].[log_store] ([log_id]),
    CONSTRAINT [FK_ingestion_source] FOREIGN KEY ([source_id]) REFERENCES [metadata].[source_store] ([source_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_ingestion_source]
    ON [instructions].[ingestion]([source_id] ASC, [is_active] ASC);


GO


-- Trigger for instructions.ingestion
CREATE   TRIGGER [instructions].trg_ingestion_audit
ON instructions.ingestion
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'instructions.ingestion', i.ingestion_id, 'insert', NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.ingestion_id = i.ingestion_id);

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'instructions.ingestion', i.ingestion_id, 'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.ingestion_id = d.ingestion_id;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'instructions.ingestion', d.ingestion_id, 'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), NULL
    FROM deleted d
    WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i.ingestion_id = d.ingestion_id);
END;

GO

