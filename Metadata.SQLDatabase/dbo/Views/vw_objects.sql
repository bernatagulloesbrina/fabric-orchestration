CREATE VIEW [dbo].[vw_objects] AS
SELECT
    w.[displayName]                                                     AS [workspace_name],
    sm.[workspace_id],
    N'Semantic Model'                                                   AS [object_type],
    N'SM'                                                               AS [object_type_code],
    sm.[id]                                                             AS [object_id],
    sm.[displayName]                                                    AS [object_name],
    w.[displayName] + N' - SM - ' + sm.[displayName]                   AS [object_label]
FROM [dbo].[semantic_models] sm
JOIN [dbo].[workspaces] w ON sm.[workspace_id] = w.[id]

UNION ALL

SELECT
    w.[displayName],
    df.[workspace_id],
    N'Dataflow',
    N'DF',
    df.[id],
    df.[displayName],
    w.[displayName] + N' - DF - ' + df.[displayName]
FROM [dbo].[dataflows] df
JOIN [dbo].[workspaces] w ON df.[workspace_id] = w.[id]

UNION ALL

SELECT
    w.[displayName],
    pl.[workspace_id],
    N'Pipeline',
    N'PL',
    pl.[id],
    pl.[displayName],
    w.[displayName] + N' - PL - ' + pl.[displayName]
FROM [dbo].[pipelines] pl
JOIN [dbo].[workspaces] w ON pl.[workspace_id] = w.[id];
