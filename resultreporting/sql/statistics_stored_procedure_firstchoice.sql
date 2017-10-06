

IF EXISTS (SELECT * FROM sysobjects WHERE name = 'FBJ_STATISTICS_ONE_SP')
    BEGIN
        DROP PROCEDURE FBJ_STATISTICS_ONE_SP;
        PRINT 'FBJ_STATISTICS_ONE_SP has been dropped.' + CHAR(10);
    END;
ELSE
    BEGIN
        PRINT 'FBJ_STATISTICS_ONE_SP does not exist yet.' + CHAR(10);
    END;
-- ENDIF;
GO



CREATE PROCEDURE FBJ_STATISTICS_ONE_SP
	@vTestName Varchar(20)
AS  
BEGIN 
SET NOCOUNT ON;

DECLARE @TestName      Varchar(20);
DECLARE @numOfTester   INT;
DECLARE @itemName      Varchar(20);
DECLARE @testerName    Varchar(50);
DECLARE @numOfFirstChoice    INT;

DECLARE @cEmail           Varchar(50);
DECLARE @cTestID          INT;
DECLARE @cResultID        INT;
DECLARE @cItem1Name       Varchar(30);
DECLARE @cItem2Name       Varchar(30);
DECLARE @cValue           INT;

DECLARE @cStaResult      TABLE(Item CHAR(20), Result CHAR(5));

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

--define item cursor for item loop
DECLARE item_cursor CURSOR FOR
	SELECT DISTINCT FBJ_ITEM.Name
	FROM FBJ_ITEM JOIN FBJ_TEST_NAME_ITEM ON FBJ_TEST_NAME_ITEM.FK_ItemID = FBJ_ITEM.PK_ItemID
				  JOIN FBJ_TEST_NAME ON FBJ_TEST_NAME.PK_TestNameID = FBJ_TEST_NAME_ITEM.FK_TestNameID
	WHERE FBJ_TEST_NAME.Name = @TestName;

BEGIN 
	SET @numOfFirstChoice = 0;

	--item loop
	OPEN item_cursor;
	FETCH item_cursor INTO @itemName;
	IF @@FETCH_STATUS = -1
        BEGIN                 
            PRINT 'item table empty'; 
        END; 
    ELSE 
        BEGIN 
            WHILE @@FETCH_STATUS = 0
                BEGIN 
                    --PRINT @itemName; 
					--reset counter;
					SET @numOfFirstChoice = 0;
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
									SELECT @maxTestID = MAX(FK_TestID) 
											FROM FBJ_RESULT JOIN FBJ_TEST ON FBJ_RESULT.FK_TestID = FBJ_TEST.PK_TestID
												JOIN FBJ_USER ON FBJ_USER.PK_UserID = FBJ_TEST.FK_UserID
												JOIN FBJ_TEST_NAME ON FBJ_TEST_NAME.PK_TestNameID = FBJ_TEST.FK_TestNameID
											WHERE FBJ_TEST_NAME.Name = @TestName
												AND FBJ_USER.Email = @testerName
											 GROUP BY FBJ_TEST.FK_UserID;

									DECLARE cal_cursor CURSOR FOR
										SELECT FBJ_USER.Email
										,FBJ_TEST.PK_TestID
										,FBJ_RESULT.PK_ResultID
										,I1.Name AS Item1
										,I2.Name AS Item2
										,FBJ_RESULT.Value
										FROM FBJ_USER    
										JOIN FBJ_TEST ON FBJ_USER.PK_UserID = FBJ_TEST.FK_UserID    
										JOIN FBJ_RESULT ON FBJ_TEST.PK_TestID = FBJ_RESULT.FK_TestID
										JOIN FBJ_ITEM I1 ON I1.PK_ItemID = FBJ_RESULT.FK_Item1ID
										JOIN FBJ_ITEM I2 ON I2.PK_ItemID = FBJ_RESULT.FK_Item2ID
										JOIN FBJ_TEST_NAME ON FBJ_TEST.FK_TestNameID = FBJ_TEST_NAME.PK_TestNameID
										WHERE FBJ_RESULT.FK_TestID =  @maxTestID
										AND FBJ_TEST_NAME.Name = @TestName AND FBJ_USER.Email = @testerName;
										--ORDER BY FBJ_USER.Email,FBJ_TEST_NAME.Name, PK_TestID, PK_ResultID;

									--tester loop
									OPEN cal_cursor;
									FETCH cal_cursor INTO @cEmail, @cTestID, @cResultID, @cItem1Name, @cItem2Name, @cValue;
									IF @@FETCH_STATUS = -1
										BEGIN
											PRINT 'cal table empty';
										END
									ELSE
										BEGIN
											WHILE @@FETCH_STATUS = 0
												BEGIN
													--PRINT  @cEmail + '  ' + @cItem1Name + '  ' + @cItem2Name + '  ' + CONVERT(Varchar(5), @cValue);
													IF @cItem1Name = @itemName AND @cValue = -1
														BEGIN
															SET @numOfFirstChoice = @numOfFirstChoice + 1;
															BREAK;
														END;
													ELSE IF @cItem2Name = @itemName AND @cValue = 1
														BEGIN 
															SET @numOfFirstChoice = @numOfFirstChoice + 1;
															BREAK;
														END;
													--ENDIF
													FETCH NEXT FROM cal_cursor INTO @cEmail, @cTestID, @cResultID, @cItem1Name, @cItem2Name, @cValue;
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
					SET @Per = CONVERT(Varchar(10), @numOfFirstChoice*100/@numOfTester)+'%';
					--PRINT CONVERT(Varchar(20), @numOfFirstChoice) + ' users selected ' + @itemName + ' first in ' + @Per+' users.';
                    INSERT @cStaResult Values(@itemName, @Per);
					FETCH NEXT FROM item_cursor INTO @itemName; 
                END;
         -- ENDWHILE;				
        END;
 -- ENDIF;
    CLOSE item_cursor;       
    DEALLOCATE item_cursor; 
	DEALLOCATE email_cursor;
	
	SELECT Item, Result FROM @cStaResult;
	
END;
	
END;
GO

PRINT 'FBJ_STATISTICS_ONE_SP has been created.';
GO


BEGIN

	PRINT 'Run a stored procedure';
    EXECUTE FBJ_STATISTICS_ONE_SP 'Animal Test';
    PRINT 'End a stored procedure';



END;
