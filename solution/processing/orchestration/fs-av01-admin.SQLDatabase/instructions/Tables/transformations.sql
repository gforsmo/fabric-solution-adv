CREATE TABLE [instructions].[transformations] (
    [transform_instr_id] INT           NOT NULL,
    [source_table]       VARCHAR (200) NOT NULL,
    [source_layer]       VARCHAR (20)  NOT NULL,
    [dest_table]         VARCHAR (200) NOT NULL,
    [dest_layer]         VARCHAR (20)  NOT NULL,
    [transform_pipeline] JSON          NOT NULL,
    [transform_params]   JSON          NULL,
    [merge_condition]    VARCHAR (500) NULL,
    [merge_type]         VARCHAR (20)  DEFAULT ('update_all') NULL,
    [merge_columns]      JSON          NULL,
    [is_active]          BIT           DEFAULT ((1)) NULL,
    [log_function_id]    INT           NULL,
    [created_date]       DATETIME2 (7) DEFAULT (getdate()) NULL,
    [modified_date]      DATETIME2 (7) DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([transform_instr_id] ASC),
    CONSTRAINT [FK_transform_log] FOREIGN KEY ([log_function_id]) REFERENCES [metadata].[log_store] ([log_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_transformations_active]
    ON [instructions].[transformations]([is_active] ASC);


GO


-- Trigger for instructions.transformations
CREATE   TRIGGER [instructions].trg_transformations_audit
ON instructions.transformations
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'instructions.transformations', i.transform_instr_id, 'insert', NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.transform_instr_id = i.transform_instr_id);

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'instructions.transformations', i.transform_instr_id, 'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.transform_instr_id = d.transform_instr_id;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'instructions.transformations', d.transform_instr_id, 'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), NULL
    FROM deleted d
    WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i.transform_instr_id = d.transform_instr_id);
END;

GO

