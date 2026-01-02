CREATE TABLE [instructions].[validations] (
    [validation_instr_id] INT           NOT NULL,
    [target_table]        VARCHAR (200) NOT NULL,
    [target_layer]        VARCHAR (20)  NOT NULL,
    [expectation_id]      INT           NOT NULL,
    [column_name]         VARCHAR (100) NULL,
    [validation_params]   JSON          NULL,
    [severity]            VARCHAR (20)  DEFAULT ('error') NULL,
    [is_active]           BIT           DEFAULT ((1)) NULL,
    [log_function_id]     INT           NULL,
    [created_date]        DATETIME2 (7) DEFAULT (getdate()) NULL,
    [modified_date]       DATETIME2 (7) DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([validation_instr_id] ASC),
    CONSTRAINT [FK_validation_expectation] FOREIGN KEY ([expectation_id]) REFERENCES [metadata].[expectation_store] ([expectation_id]),
    CONSTRAINT [FK_validation_log] FOREIGN KEY ([log_function_id]) REFERENCES [metadata].[log_store] ([log_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_validations_table]
    ON [instructions].[validations]([target_table] ASC, [is_active] ASC);


GO


-- Trigger for instructions.validations
CREATE   TRIGGER [instructions].trg_validations_audit
ON instructions.validations
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'instructions.validations', i.validation_instr_id, 'insert', NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.validation_instr_id = i.validation_instr_id);

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'instructions.validations', i.validation_instr_id, 'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.validation_instr_id = d.validation_instr_id;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'instructions.validations', d.validation_instr_id, 'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), NULL
    FROM deleted d
    WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i.validation_instr_id = d.validation_instr_id);
END;

GO

