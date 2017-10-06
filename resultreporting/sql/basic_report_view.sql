ALTER VIEW FBJ_BASIC_REPORT
AS
SELECT FBJ_USER.Email
,UnionResult.FK_TestID
,I1.Name AS Item1
,ISNULL(SUM(CASE WHEN UnionResult.Value = -1 THEN 1 END), 0) AS Wins
,ISNULL(SUM(CASE WHEN UnionResult.Value = 1 THEN 1 END), 0) AS Losses
,ISNULL(SUM(CASE WHEN UnionResult.Value = 0 THEN 1 END), 0) AS Ties
,SUM(UnionResult.Value)*(-1) AS Points
FROM FBJ_USER    
JOIN FBJ_TEST ON FBJ_USER.PK_UserID = FBJ_TEST.FK_UserID
JOIN (SELECT FK_TestID, FK_Item1ID AS ITEM1, FK_Item2ID AS ITEM2, Value FROM FBJ_RESULT
		UNION ALL
	  SELECT FK_TestID, FK_Item2ID AS ITEM1, FK_Item1ID AS ITEM2, -1*Value  FROM FBJ_RESULT ) AS UnionResult
	  ON FBJ_TEST.PK_TestID = UnionResult.FK_TestID
JOIN FBJ_ITEM I1 ON I1.PK_ItemID = UnionResult.ITEM1
GROUP BY  FBJ_USER.Email, UnionResult.FK_TestID, I1.Name;