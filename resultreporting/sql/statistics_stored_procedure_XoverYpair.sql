

IF EXISTS (SELECT * FROM sysobjects WHERE name = 'FBJ_STATISTICS_XOVERY_SP')
    BEGIN
        DROP PROCEDURE FBJ_STATISTICS_XOVERY_SP;
        PRINT 'FBJ_STATISTICS_XOVERY_SP has been dropped.' + CHAR(10);
    END;
ELSE
    BEGIN
        PRINT 'FBJ_STATISTICS_XOVERY_SP does not exist yet.' + CHAR(10);
    END;
-- ENDIF;
GO



CREATE PROCEDURE FBJ_STATISTICS_XOVERY_SP
	@vTestName Varchar(20)
AS  
BEGIN 
SET NOCOUNT ON;

DECLARE @TestName      Varchar(20);
DECLARE @numOfTester   INT;
DECLARE @item1Name      Varchar(20);
DECLARE @item2Name      Varchar(20);
DECLARE @testerName    Varchar(50);
DECLARE @numOfXOverY    INT;
DECLARE @numOfitems       INT;

DECLARE @cEmail           Varchar(50);
DECLARE @cTestID          INT;
DECLARE @cResultID        INT;
DECLARE @cItem1Name       Varchar(30);
DECLARE @cItem2Name       Varchar(30);
DECLARE @cValue           INT;


DECLARE @cStaXOverY      TABLE(Email CHAR(30), TestID INT, Item1 CHAR(30), Item2 CHAR(30), Value INT, TestName CHAR(20));
DECLARE @cStaResult      TABLE(ItemX CHAR(20), ItemY CHAR(20), Result CHAR(5));

--set parameter
SET @TestName = @vTestName;

--get number of users
SELECT @numOfTester = COUNT(DISTINCT FK_UserID)
FROM FBJ_TEST 
JOIN FBJ_TEST_NAME ON FBJ_TEST.FK_TestNameID = FBJ_TEST_NAME.PK_TestNameID
WHERE FBJ_TEST_NAME.Name = @TestName;

-- define email cursor for email loop
DECLARE email_cursor CURSOR FOR
	SELECT DISTINCT FBJ_USER.Email
	FROM FBJ_USER JOIN FBJ_TEST ON FBJ_TEST.FK_UserID = FBJ_USER.PK_UserID
				  JOIN FBJ_TEST_NAME ON FBJ_TEST.FK_TestNameID = FBJ_TEST_NAME.PK_TestNameID
	WHERE  FBJ_TEST_NAME.Name = @TestName;


--fill x over y table
INSERT INTO @cStaXOverY  SELECT FBJ_USER.Email
,UnionResult.FK_TestID
,I1.Name AS Item1
,I2.Name AS Item2
,UnionResult.Value
,FBJ_TEST_NAME.Name
FROM FBJ_USER    
JOIN FBJ_TEST ON FBJ_USER.PK_UserID = FBJ_TEST.FK_UserID    
JOIN (SELECT FK_TestID, FK_Item1ID AS ITEM1, FK_Item2ID AS ITEM2, Value FROM FBJ_RESULT
		UNION ALL
	  SELECT FK_TestID, FK_Item2ID AS ITEM1, FK_Item1ID AS ITEM2, -1*Value  FROM FBJ_RESULT ) AS UnionResult
		ON FBJ_TEST.PK_TestID = UnionResult.FK_TestID
JOIN FBJ_ITEM I1 ON I1.PK_ItemID = UnionResult.ITEM1
JOIN FBJ_ITEM I2 ON I2.PK_ItemID = UnionResult.ITEM2
JOIN FBJ_TEST_NAME ON FBJ_TEST_NAME.PK_TestNameID = FBJ_TEST.FK_TestNameID;

--define item cursor for item loop
DECLARE item_cursor CURSOR FOR
	SELECT Item1, Item2 
	FROM @cStaXOverY 
	WHERE TestName = @TestName AND TestID  IN (SELECT MAX(PK_TestID) FROM FBJ_TEST GROUP BY FBJ_TEST.FK_TestNameID)
	ORDER BY Item1, Item2;
	

BEGIN 
	SET @numOfXOverY = 0;

	--item loop
	OPEN item_cursor;
	FETCH item_cursor INTO @item1Name, @item2Name;
	IF @@FETCH_STATUS = -1
        BEGIN                 
            PRINT 'item table empty'; 
        END; 
    ELSE 
        BEGIN 
            WHILE @@FETCH_STATUS = 0
                BEGIN 
                    --PRINT @item1Name + ' ' +  @item2Name; 
					--reset counter;
					SET @numOfXOverY = 0;
					--email loop
					OPEN email_cursor; 
					FETCH email_cursor INTO @testerName;
					IF @@FETCH_STATUS = -1
						BEGIN                 
							PRINT 'tester table empty'; 
						END; 
					ELSE
						BEGIN 
							WHILE @@FETCH_STATUS = 0
								BEGIN
									--PRINT @testerName; 
									--define statistics cursor for cal loop
									DECLARE @maxTestID INT;
									SELECT @maxTestID = MAX(PK_TestID) 
									FROM FBJ_TEST JOIN FBJ_USER ON FBJ_USER.PK_UserID = FBJ_TEST.FK_UserID   
												  JOIN FBJ_TEST_NAME ON FBJ_TEST_NAME.PK_TestNameID = FBJ_TEST.FK_TestNameID
									WHERE FBJ_TEST_NAME.Name = @TestName AND FBJ_USER.Email = @testerName
									GROUP BY FBJ_USER.Email;

									DECLARE cal_cursor CURSOR FOR
										SELECT Email, Item1, Item2, Value
										FROM @cStaXOverY
										WHERE TestID =  @maxTestID
										AND TestName = @TestName AND Email = @testerName;

									--tester loop
									OPEN cal_cursor;
									FETCH cal_cursor INTO @cEmail, @cItem1Name, @cItem2Name, @cValue;
									IF @@FETCH_STATUS = -1
										BEGIN
											PRINT 'cal table empty';
										END
									ELSE
										BEGIN
											WHILE @@FETCH_STATUS = 0
												BEGIN
													--PRINT  @cEmail + '  ' + @cItem1Name + '  ' + @cItem2Name + '  ' + CONVERT(Varchar(5), @cValue);
													IF @cItem1Name = @item1Name AND @cItem2Name = @item2Name AND @cValue = -1
														BEGIN
															SET @numOfXOverY = @numOfXOverY + 1;
															BREAK;
														END;
													--ENDIF
													FETCH NEXT FROM cal_cursor INTO @cEmail, @cItem1Name, @cItem2Name, @cValue;
												END;
											--ENDWHILE
										END;
									--ENDIF
									CLOSE cal_cursor;
									DEALLOCATE cal_cursor;
									FETCH NEXT FROM email_cursor INTO @testerName; 
								END;
							--ENDWHILE
						END;
					--ENDIF
					CLOSE email_cursor;
					DECLARE @Per CHAR(10);
					SET @Per = CONVERT(Varchar(10), @numOfXOverY*100/@numOfTester)+'%';
					--PRINT CONVERT(Varchar(20), @numOfXOverY) + ' users selected ' + @itemName + ' first over ' + @item2Name + 'in ' + @Per+' users.';
                    INSERT @cStaResult Values(RTRIM(LTRIM(@item1Name)), RTRIM(LTRIM(@item2Name)), @Per);
					FETCH NEXT FROM item_cursor INTO @item1Name, @item2Name; 
                END;
         -- ENDWHILE;				
        END;
 -- ENDIF;
    CLOSE item_cursor;       
    DEALLOCATE item_cursor; 
	DEALLOCATE email_cursor;
	
	SELECT ItemX, ItemY, Result FROM @cStaResult;
	
END;
	
END;
GO

PRINT 'FBJ_STATISTICS_XOVERY_SP has been created.';
GO


BEGIN

	PRINT 'Run a stored procedure';
    EXECUTE FBJ_STATISTICS_XOVERY_SP 'Animal Test';
    PRINT 'End a stored procedure';



END;
